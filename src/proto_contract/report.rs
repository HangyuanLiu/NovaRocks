use prost::Message;

use crate::proto::novarocks;
use crate::runtime::profile::RuntimeProfile;
use crate::thrift::metrics;

fn roundtrip_message<M>(value: &M) -> M
where
    M: Message + Default,
{
    M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
}

#[test]
fn runtime_profile_tree_survives_proto_roundtrip() {
    let root = RuntimeProfile::new("FragmentRoot");
    root.set_metadata(10);
    root.add_info_string("query_id", "q-1");

    let z_root = root.add_unit_counter("ZRoot");
    z_root.set(300);
    let none_counter = root.add_child_counter("NoUnitCounter", metrics::TUnit::NONE, "ZRoot");
    none_counter.set(0);

    let total_time = root.add_timer("TotalTime");
    total_time.set(123);
    total_time.set_min(100);
    total_time.set_max(200);

    let scan_time = root.add_child_timer("ScanTime", "TotalTime");
    scan_time.set(70);
    scan_time.set_min(60);
    scan_time.set_max(90);

    let scan = root.child("SCAN (plan_node_id=1)");
    scan.set_metadata(1);
    scan.add_info_string("table", "lineitem");
    scan.counter_set_bytes("DataCacheReadBytes", 4096);

    let rows_read = scan.add_child_counter("RowsRead", metrics::TUnit::UNIT, "DataCacheReadBytes");
    rows_read.set(8);
    rows_read.set_min(4);
    rows_read.set_max(12);

    let exchange = root.child("EXCHANGE (plan_node_id=2)");
    exchange.set_metadata(2);
    exchange.add_info_string("partition", "HASH");
    exchange.counter_set("NetworkTime", metrics::TUnit::TIME_MS, 9);

    let decoded: novarocks::RuntimeProfileTree = roundtrip_message(&root.to_proto());
    let decoded_root = decoded.root.expect("profile root");
    assert_eq!(decoded_root.name, "FragmentRoot");
    assert_eq!(decoded_root.node_id, 10);
    assert_eq!(
        decoded_root.info_strings.get("query_id"),
        Some(&"q-1".to_string())
    );
    assert_eq!(
        decoded_root
            .children
            .iter()
            .map(|child| child.name.as_str())
            .collect::<Vec<_>>(),
        vec!["SCAN (plan_node_id=1)", "EXCHANGE (plan_node_id=2)"]
    );
    assert_eq!(
        decoded_root
            .counters
            .iter()
            .map(|counter| (counter.parent_name.as_str(), counter.name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("", "TotalTime"),
            ("", "ZRoot"),
            ("TotalTime", "ScanTime"),
            ("ZRoot", "NoUnitCounter"),
        ]
    );

    let root_total = decoded_root
        .counters
        .iter()
        .find(|c| c.name == "TotalTime")
        .expect("TotalTime counter");
    assert_eq!(root_total.parent_name, "");
    assert_eq!(root_total.unit, novarocks::ProfileUnit::TimeNs as i32);
    assert_eq!(root_total.value, 123);
    assert_eq!(root_total.min_value, Some(100));
    assert_eq!(root_total.max_value, Some(200));

    let root_scan_time = decoded_root
        .counters
        .iter()
        .find(|c| c.name == "ScanTime")
        .expect("ScanTime counter");
    assert_eq!(root_scan_time.parent_name, "TotalTime");
    assert_eq!(root_scan_time.unit, novarocks::ProfileUnit::TimeNs as i32);
    assert_eq!(root_scan_time.min_value, Some(60));
    assert_eq!(root_scan_time.max_value, Some(90));

    let no_unit_counter = decoded_root
        .counters
        .iter()
        .find(|c| c.name == "NoUnitCounter")
        .expect("NoUnitCounter counter");
    assert_eq!(no_unit_counter.parent_name, "ZRoot");
    assert_eq!(no_unit_counter.unit, novarocks::ProfileUnit::None as i32);

    let scan_node = decoded_root
        .children
        .iter()
        .find(|child| child.name == "SCAN (plan_node_id=1)")
        .expect("scan child");
    assert_eq!(scan_node.node_id, 1);
    assert_eq!(
        scan_node.info_strings.get("table"),
        Some(&"lineitem".to_string())
    );
    let scan_bytes = scan_node
        .counters
        .iter()
        .find(|c| c.name == "DataCacheReadBytes")
        .expect("DataCacheReadBytes counter");
    assert_eq!(scan_bytes.parent_name, "");
    assert_eq!(scan_bytes.unit, novarocks::ProfileUnit::Bytes as i32);
    assert_eq!(scan_bytes.value, 4096);

    let rows_read = scan_node
        .counters
        .iter()
        .find(|c| c.name == "RowsRead")
        .expect("RowsRead counter");
    assert_eq!(rows_read.parent_name, "DataCacheReadBytes");
    assert_eq!(rows_read.unit, novarocks::ProfileUnit::Unit as i32);
    assert_eq!(rows_read.value, 8);
    assert_eq!(rows_read.min_value, Some(4));
    assert_eq!(rows_read.max_value, Some(12));

    let exchange_node = decoded_root
        .children
        .iter()
        .find(|child| child.name == "EXCHANGE (plan_node_id=2)")
        .expect("exchange child");
    assert_eq!(
        exchange_node.info_strings.get("partition"),
        Some(&"HASH".to_string())
    );
    let network_time = exchange_node
        .counters
        .iter()
        .find(|c| c.name == "NetworkTime")
        .expect("NetworkTime counter");
    assert_eq!(network_time.unit, novarocks::ProfileUnit::TimeMs as i32);
    assert_eq!(network_time.value, 9);
}
