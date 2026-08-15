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

pub use crate::query_execution::native_fragment::{
    NativeFragmentAttachment as NativeFragmentBundle,
    NativeFragmentEncodingView as NativeFragmentEncodingSource,
};

#[cfg(feature = "query-execution-contract-test-support")]
pub(crate) use crate::query_execution::native_fragment::native_fragment_attachment_for_contract_test as native_fragment_bundle_for_contract_test;

/// Encode one immutable distributed plan and its exact prepared bindings into
/// the native FE-to-BE wire bundle.
pub fn encode_native_fragment_bundle(
    source: NativeFragmentEncodingSource<'_>,
) -> Result<NativeFragmentBundle, String> {
    let plan = source.distributed_plan();
    let scan_facts = source.scan_facts();
    let encoded = super::plan::encode_distributed_plan_from_prepared(
        plan,
        scan_facts.bindings(),
        source.prepared(),
    )?;
    source.seal(encoded.fragments)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::query_execution::native_fragment::native_fragment_attachment_for_test as collect_native_fragment_bundle;
    use novarocks_protocol::plan::PlanFragment as NativePlanFragment;
    use novarocks_sql::plan_read::FragmentId;

    fn fragment(fragment_id: FragmentId) -> NativePlanFragment {
        NativePlanFragment {
            fragment_id,
            ..Default::default()
        }
    }

    fn try_bundle_for_test(
        fragment_ids: &[FragmentId],
        expected_ids: &BTreeSet<FragmentId>,
    ) -> Result<NativeFragmentBundle, String> {
        collect_native_fragment_bundle(
            fragment_ids.iter().copied().map(fragment),
            expected_ids,
            None,
        )
    }

    #[test]
    fn ordered_views_and_get_are_narrow_and_stable() {
        let expected = BTreeSet::from([2, 11, 23]);
        let bundle = try_bundle_for_test(&[23, 2, 11], &expected).expect("unique bundle");
        assert_eq!(bundle.fragment_ids().collect::<Vec<_>>(), vec![2, 11, 23]);
        assert_eq!(
            bundle
                .fragments_in_id_order()
                .map(|(fragment_id, fragment)| (fragment_id, fragment.fragment_id))
                .collect::<Vec<_>>(),
            vec![(2, 2), (11, 11), (23, 23)]
        );
        assert_eq!(
            bundle.get(11).map(|fragment| fragment.fragment_id),
            Some(11)
        );
        assert!(bundle.get(99).is_none());
    }

    #[test]
    fn into_fragments_consumes_every_payload_once_in_id_order() {
        let expected = BTreeSet::from([3, 7]);
        let bundle = try_bundle_for_test(&[7, 3], &expected).expect("unique bundle");
        assert_eq!(
            bundle
                .into_fragments()
                .map(|(fragment_id, fragment)| (fragment_id, fragment.fragment_id))
                .collect::<Vec<_>>(),
            vec![(3, 3), (7, 7)]
        );
    }

    #[test]
    fn sealed_bundle_provenance_rejects_a_different_encoding_input() {
        let expected = BTreeSet::from([7]);
        let bundle = collect_native_fragment_bundle([fragment(7)], &expected, Some(41))
            .expect("sealed bundle");
        assert!(bundle.matches_provenance(41));
        assert!(!bundle.matches_provenance(42));
    }

    #[test]
    fn duplicate_missing_and_unknown_ids_fail_exactly() {
        let duplicate = try_bundle_for_test(&[3, 3], &BTreeSet::from([3]))
            .err()
            .expect("duplicate id must fail");
        assert_eq!(
            duplicate,
            "native fragment bundle encoded duplicate fragment id=3"
        );

        let expected = BTreeSet::from([2, 11]);
        assert_eq!(
            try_bundle_for_test(&[2, 23], &expected)
                .err()
                .expect("missing/unknown ids must fail"),
            "native fragment ids mismatch: expected={2, 11} actual={2, 23} missing=[11] unknown=[23]"
        );
    }

    #[test]
    fn native_fragment_ownership_rejects_missing_fragment_and_root() {
        let expected = BTreeSet::from([0, 1]);
        let missing_fragment = collect_native_fragment_bundle([fragment(0)], &expected, None)
            .err()
            .expect("missing scheduled fragment must fail through production collector");
        assert!(missing_fragment.contains("native fragment ids mismatch"));
        assert!(missing_fragment.contains("missing=[1]"));

        let missing_root = collect_native_fragment_bundle([fragment(1)], &expected, None)
            .err()
            .expect("missing root fragment must fail through production collector");
        assert!(missing_root.contains("missing=[0]"), "{missing_root}");
    }
}
