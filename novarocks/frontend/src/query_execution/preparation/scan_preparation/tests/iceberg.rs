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

//! Typed connector lowering of Iceberg scans.

use super::*;

/// `ScanExecutionBindings` holds connector leases and split managers, so it is
/// deliberately not `Debug`; a refusal is therefore asserted through a match.
fn expect_preparation_error(
    result: Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String>,
    expectation: &str,
) -> String {
    match result {
        Ok(_) => panic!("{expectation}"),
        Err(error) => error,
    }
}

/// The one scan node every single-scan fixture plan has.
fn only_scan_node(
    bindings: &crate::query_execution::preparation::scan::ScanExecutionBindings,
) -> (novarocks_sql::plan_read::FragmentId, i32) {
    let mut keys = bindings.typed_scan_keys().collect::<Vec<_>>();
    assert_eq!(keys.len(), 1, "the fixture plan has exactly one scan");
    keys.pop().expect("one typed scan")
}

#[test]
fn an_ordinary_iceberg_scan_lowers_to_a_typed_data_relation() {
    let bindings = prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
            .expect("sealed ordinary iceberg fixture"),
        &registry(vec![data_file("s3://bucket/data.parquet")]),
        None,
    )
    .expect("typed scan preparation");

    let (fragment_id, node_id) = only_scan_node(&bindings);
    let typed = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan");
    assert_eq!(
        typed.prepared.table_scan.table().relation_kind(),
        novarocks_proto::connector_read::ConnectorRelationKind::Table
    );
    assert_eq!(
        typed.prepared.table_scan.table().catalog().instance_id(),
        typed.declaration.binding_key().instance_id.as_str(),
        "the frozen relation and its declaration name one generation"
    );
}

/// Preparation must hand the connector's enumerator on untouched. Nothing here
/// may produce a split set, and in particular the opaque carrier — the only
/// thing that ever carried one — must have no entry at all.
#[test]
fn preparation_enumerates_no_split() {
    let bindings = prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
            .expect("sealed ordinary iceberg fixture"),
        &registry(vec![
            data_file("s3://bucket/a.parquet"),
            data_file("s3://bucket/b.parquet"),
        ]),
        None,
    )
    .expect("typed scan preparation");

    assert!(
        bindings.connector_reads().next().is_none(),
        "no opaque connector read with a frozen split list may be produced"
    );
    let (fragment_id, node_id) = only_scan_node(&bindings);
    let typed = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan");
    // The fixture control fails any enumeration attempt, so a prepared scan
    // proves preparation never called it.
    assert!(
        typed
            .prepared
            .split_manager
            .get_splits(
                &novarocks_spi::connector::read_stack::ConnectorSession::try_new(
                    "probe",
                    "probe",
                    "UTC",
                    "en_US",
                    std::time::SystemTime::UNIX_EPOCH,
                )
                .expect("probe session"),
                typed.prepared.table_scan.source().table(),
                typed.prepared.table_scan.source().assignments(),
                &typed.prepared.table_scan.dynamic_filter_columns(),
                &typed.prepared.constraint,
            )
            .is_err(),
        "the fixture enumerator refuses, so preparation cannot have used it"
    );
}

/// The ordered assignments are the output-order authority, so they follow the
/// scan's own output order and are never sorted into the connector's order.
#[test]
fn assignments_follow_the_scan_output_order() {
    let bindings = prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
            .expect("sealed ordinary iceberg fixture"),
        &registry(vec![data_file("s3://bucket/data.parquet")]),
        None,
    )
    .expect("typed scan preparation");
    let (fragment_id, node_id) = only_scan_node(&bindings);
    let typed = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan");
    let assignments = typed.prepared.table_scan.source().assignments();
    assert!(!assignments.is_empty());
    for (ordinal, assignment) in assignments.iter().enumerate() {
        assert_eq!(assignment.variable(), format!("v{ordinal}"));
    }
}

/// An MV target scan is an ordinary pinned DATA read: the lane it came from is
/// not an execution difference the typed stack can observe.
#[test]
fn an_mv_target_scan_lowers_to_an_ordinary_pinned_data_handle() {
    for fixture in [
        NativeScanFixture::TargetStateProjection,
        NativeScanFixture::TargetLocatorProjection,
    ] {
        let plan = native_scan_plan(fixture).expect("sealed MV target fixture");
        let bindings = prepare_scan_bindings(
            &plan,
            &registry(vec![data_file("s3://bucket/target.parquet")]),
            None,
        )
        .unwrap_or_else(|error| panic!("MV target lowering for {fixture:?}: {error}"));
        let (fragment_id, node_id) = only_scan_node(&bindings);
        let typed = bindings
            .typed_scan(fragment_id, node_id)
            .expect("typed connector scan");
        assert_eq!(
            typed.prepared.table_scan.table().relation_kind(),
            novarocks_proto::connector_read::ConnectorRelationKind::Table,
            "an MV target lane must not produce a specialized relation"
        );
    }
}

/// A change-window read is its own relation family with its own enumerator.
/// It must refuse by name instead of silently reaching the opaque carrier.
#[test]
fn a_change_window_scan_is_a_typed_unsupported_relation_error() {
    let error = expect_preparation_error(
        prepare_scan_bindings(
            &native_scan_plan(NativeScanFixture::DeltaForPreparedBinding)
                .expect("sealed delta fixture"),
            &registry(vec![data_file("s3://bucket/data.parquet")]),
            None,
        ),
        "a change-window scan has no typed lowering",
    );
    assert!(
        error.contains("change_window") && error.contains("does not admit"),
        "unexpected error: {error}"
    );
}

/// A pre-pinned opaque read has no relation name or version left to freeze, so
/// it fails closed rather than emitting the carrier it used to.
#[test]
fn a_pre_pinned_opaque_read_fails_closed() {
    let error = expect_preparation_error(
        prepare_scan_bindings(
            &native_scan_plan(NativeScanFixture::ConnectorRead)
                .expect("sealed connector-read fixture"),
            &registry(vec![data_file("s3://bucket/data.parquet")]),
            Some(&StaticResolver {
                execution: ResolvedScanExecution::ConnectorRead,
            }),
        ),
        "a pre-pinned opaque read has no typed lowering",
    );
    assert!(
        error.contains("pre-pinned opaque connector read"),
        "unexpected error: {error}"
    );
}

/// A scan whose binding generation is not installed must fail closed: an
/// absent typed control is never permission to reach some other generation.
#[test]
fn a_scan_whose_binding_generation_does_not_resolve_fails_closed() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary iceberg fixture");
    let connectors = registry(vec![data_file("s3://bucket/data.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    let query_bindings = fixture_query_table_bindings(&plan, &controls);
    // An empty registry stands for "this generation was never installed".
    let empty =
        Arc::new(crate::connector::typed_control_registry::TypedConnectorControlRegistry::new());
    let error = expect_preparation_error(
        super::super::prepare_scan_bindings(
            &plan,
            &controls,
            &crate::connector::test_request_context(),
            Some(&query_bindings),
            None,
            &fixture_scan_preparation_options(empty),
        ),
        "an uninstalled generation cannot be planned",
    );
    assert!(
        error.contains("no typed connector control is installed for this exact generation"),
        "unexpected error: {error}"
    );
}

/// Preparation without the statement's typed control inputs is a contract
/// error, not permission to fall back to the opaque carrier.
#[test]
fn preparation_without_typed_inputs_refuses_instead_of_falling_back() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary iceberg fixture");
    let connectors = registry(vec![data_file("s3://bucket/data.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    let query_bindings = fixture_query_table_bindings(&plan, &controls);
    let error = expect_preparation_error(
        super::super::prepare_scan_bindings(
            &plan,
            &controls,
            &crate::connector::test_request_context(),
            Some(&query_bindings),
            None,
            &super::super::ScanPreparationOptions::single_backend_fixture(),
        ),
        "no typed control registry was threaded in",
    );
    assert!(
        error.contains("typed control registry"),
        "unexpected error: {error}"
    );
}

/// A frozen snapshot selector reaches the connector as an exact pin.
#[test]
fn a_frozen_snapshot_scan_pins_its_admitted_snapshot() {
    let bindings = prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::FrozenSnapshotEleven)
            .expect("sealed frozen-snapshot fixture"),
        &registry(vec![data_file("s3://bucket/data.parquet")]),
        None,
    )
    .expect("typed scan preparation");
    let (fragment_id, node_id) = only_scan_node(&bindings);
    let typed = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan");
    // The fixture control echoes the requested version into the handle it
    // freezes, so the pinned snapshot is observable from the carrier.
    let novarocks_proto::connector_read::ConnectorRelation::Table(table) =
        typed.prepared.table_scan.table().handle().relation()
    else {
        panic!("a DATA scan freezes a table relation");
    };
    let Some(novarocks_proto_models::connector_read::connector_table_handle::Handle::Iceberg(
        iceberg,
    )) = table.handle.as_ref()
    else {
        panic!("the fixture control freezes an Iceberg table handle");
    };
    assert_eq!(iceberg.snapshot_id, Some(11));
}
