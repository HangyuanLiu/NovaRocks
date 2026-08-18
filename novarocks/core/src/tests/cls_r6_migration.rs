//! Temporary CLS-R6 migration fixtures.
//!
//! Core still owns the historical parallel values while the consumer cutover
//! is in flight. Keep the old-to-generated projections here so Protocol never
//! gains a reverse dependency. T5 removes this module with the old values.

use prost::Message;

use crate::query_lifecycle::FragmentTerminalOutcome as CoreFragmentTerminalOutcome;
use novarocks_protocol::lifecycle::{FragmentTerminalOutcome, QueryOptions};
use novarocks_protocol::{common, novarocks};

fn project_historical_outcome(
    outcome: &CoreFragmentTerminalOutcome,
) -> novarocks::TerminalizationProofFragment {
    let (outcome, error_code, error_detail, error_detail_truncated) = match outcome {
        CoreFragmentTerminalOutcome::Succeeded => (
            novarocks::QueryTerminalFragmentOutcome::Succeeded as i32,
            String::new(),
            String::new(),
            false,
        ),
        CoreFragmentTerminalOutcome::Failed {
            code,
            detail,
            detail_truncated,
        } => (
            novarocks::QueryTerminalFragmentOutcome::Failed as i32,
            code.clone(),
            detail.clone(),
            *detail_truncated,
        ),
        CoreFragmentTerminalOutcome::Cancelled {
            detail,
            detail_truncated,
        } => (
            novarocks::QueryTerminalFragmentOutcome::Cancelled as i32,
            String::new(),
            detail.clone(),
            *detail_truncated,
        ),
        CoreFragmentTerminalOutcome::IncompleteDrain {
            detail,
            detail_truncated,
        } => (
            novarocks::QueryTerminalFragmentOutcome::IncompleteDrain as i32,
            String::new(),
            detail.clone(),
            *detail_truncated,
        ),
    };

    novarocks::TerminalizationProofFragment {
        fragment_instance_id: Some(common::UniqueId { hi: 7, lo: 9 }),
        backend_num: 3,
        outcome,
        error_code,
        error_detail,
        error_detail_truncated,
    }
}

#[test]
fn historical_terminal_outcome_projects_to_fixed_protocol_bytes() {
    let historical = CoreFragmentTerminalOutcome::Failed {
        code: "FRAGMENT_EXECUTION_FAILED".into(),
        detail: "fixture failure".into(),
        detail_truncated: false,
    };

    let generated = project_historical_outcome(&historical);
    let protocol = FragmentTerminalOutcome::parse(generated.clone()).expect("valid projection");

    assert_eq!(
        protocol.kind(),
        novarocks::QueryTerminalFragmentOutcome::Failed
    );
    assert_eq!(protocol.error_code(), "FRAGMENT_EXECUTION_FAILED");
    assert_eq!(protocol.error_detail(), "fixture failure");
    assert_eq!(
        generated.encode_to_vec(),
        [
            10, 4, 8, 7, 16, 9, 16, 3, 24, 2, 34, 25, 70, 82, 65, 71, 77, 69, 78, 84, 95, 69, 88,
            69, 67, 85, 84, 73, 79, 78, 95, 70, 65, 73, 76, 69, 68, 42, 15, 102, 105, 120, 116,
            117, 114, 101, 32, 102, 97, 105, 108, 117, 114, 101,
        ]
    );
}

#[test]
fn protocol_query_options_retain_the_historical_core_decoder_input() {
    let raw = novarocks::QueryOptions {
        batch_size: 4096,
        pipeline_dop: 8,
        query_mem_limit: 1024,
        enable_parquet_reader_page_index: true,
        ..Default::default()
    };

    let protocol = QueryOptions::parse(raw.clone()).expect("valid protocol options");
    let decoded = crate::protocol::decode_native_query_options(protocol.as_proto())
        .expect("historical core decoder accepts the protocol contract");

    assert_eq!(decoded.batch_size, Some(4096));
    assert_eq!(decoded.pipeline_dop, Some(8));
    assert_eq!(decoded.exec_mem_limit, Some(1024));
    assert!(decoded.enable_parquet_reader_page_index);
    assert_eq!(protocol.as_proto().encode_to_vec(), raw.encode_to_vec());
}
