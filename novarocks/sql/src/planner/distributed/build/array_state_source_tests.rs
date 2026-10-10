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

//! Real lexical ARRAY producers and the existing state journal consumer facet.
// Mounted as a child of physical_aggregate_journal_update_tests, so every
// source loan comes from the actual completion driver, never a fabricated entry.
use super::*;

#[test]
fn array_state_source_real_ordered_distinct_update_and_final_receipts() {
    for (sql, distinct, ascending, nulls_first) in [
        (
            "SELECT ARRAY_AGG(order_key ORDER BY order_key DESC NULLS FIRST) FROM orders",
            false,
            false,
            true,
        ),
        (
            "SELECT ARRAY_AGG(DISTINCT order_key ORDER BY order_key ASC NULLS LAST) FROM orders",
            true,
            true,
            false,
        ),
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        let partial = loan(&owner, true);
        let original = partial
            .captured()
            .binding()
            .aggregate_state_source()
            .unwrap();
        assert_eq!(original.distinct, distinct);
        assert_eq!(
            original.order_keys.as_ref(),
            [novarocks_type_contract::AggregateStateOrderKey {
                ascending,
                nulls_first
            }]
        );
        assert!(partial.captured().binding().group_concat_source().is_none());
        assert_eq!(
            partial.source().binding.state_interpretation.as_ref(),
            Some(original)
        );
        let update = run(&partial, &Control::default()).unwrap();
        let PureCallPreparation::Aggregate { options, .. } = update.preparation(effects()) else {
            panic!("Partial preparation")
        };
        assert_eq!(options.phase, AggregateKernelPhase::Partial);
        assert_eq!(options.distinct, distinct);
        assert_eq!(options.order_keys.len(), 1);
        assert_eq!(options.state_interpretation.as_deref(), Some(original));
        let final_entry = loan(&owner, false);
        assert!(!final_entry.source().distinct);
        assert!(final_entry.source().order_by.is_empty());
        let control = Control::default();
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        let merge =
            author_physical_aggregate_merge_request_observed(&final_entry, &mut work).unwrap();
        let mut actual_producers = 0;
        merge
            .state_inputs()
            .visit_observed(
                &mut work,
                |producer, _, work| {
                    actual_producers += 1;
                    assert_eq!(
                        producer.captured().binding().aggregate_state_source(),
                        Some(original)
                    );
                    assert_eq!(
                        producer.source().binding.state_interpretation.as_ref(),
                        Some(original)
                    );
                    work.step()?;
                    Ok(())
                },
                |_, _, _, _| Ok(()),
            )
            .unwrap();
        assert!(actual_producers > 0);
        let PureCallPreparation::Aggregate { options, .. } = merge.preparation(effects()) else {
            panic!("Final preparation")
        };
        assert_eq!(options.phase, AggregateKernelPhase::Final);
        assert!(!options.distinct);
        assert!(options.order_keys.is_empty());
        assert_eq!(options.state_interpretation.as_deref(), Some(original));
        work.finish().unwrap();
    }
}

#[test]
fn array_state_source_actual_plain_generic_aggregate_keeps_plain_execution_flags() {
    let owner =
        crate::compiler::compile_authored_aggregate_for_test("SELECT MIN(order_key) FROM orders");
    let partial = loan(&owner, true);
    let facts = partial
        .captured()
        .binding()
        .aggregate_state_source()
        .unwrap();
    assert!(!facts.distinct);
    assert!(facts.order_keys.is_empty());
    let update = run(&partial, &Control::default()).unwrap();
    let PureCallPreparation::Aggregate { options, .. } = update.preparation(effects()) else {
        panic!("Partial preparation")
    };
    assert!(!options.distinct);
    assert!(options.order_keys.is_empty());
    assert_eq!(options.state_interpretation.as_deref(), Some(facts));
}
