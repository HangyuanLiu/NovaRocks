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

use std::collections::{BTreeMap, BTreeSet, btree_map};

use crate::query_execution::preparation::PreparedFragmentSet;
use novarocks_protocol::plan::PlanFragment as NativePlanFragment;
use novarocks_sql::plan_read::{DistributedPlan, FragmentId};

/// Immutable encoder borrow for either a Core-sealed application assembly or
/// a Core-internal neutral execution flow. Only the sealed constructor carries
/// a provenance token that a retained assembly can later verify.
pub struct NativeFragmentEncodingSource<'a> {
    plan: &'a DistributedPlan,
    prepared: &'a PreparedFragmentSet,
    provenance: Option<u64>,
}

impl<'a> NativeFragmentEncodingSource<'a> {
    pub(crate) fn sealed(
        plan: &'a DistributedPlan,
        prepared: &'a PreparedFragmentSet,
        provenance: u64,
    ) -> Self {
        Self {
            plan,
            prepared,
            provenance: Some(provenance),
        }
    }

    pub(crate) fn unsealed(plan: &'a DistributedPlan, prepared: &'a PreparedFragmentSet) -> Self {
        Self {
            plan,
            prepared,
            provenance: None,
        }
    }
}

/// Sealed native DTOs for one fully prepared distributed plan. Construction is
/// restricted to the encoder so application owners cannot synthesize wire
/// fragments or alter their bytes after encoding.
pub struct NativeFragmentBundle {
    by_fragment: BTreeMap<FragmentId, NativePlanFragment>,
    provenance: Option<u64>,
}

impl NativeFragmentBundle {
    pub(crate) fn fragment_ids(&self) -> impl ExactSizeIterator<Item = FragmentId> + '_ {
        self.by_fragment.keys().copied()
    }

    pub(crate) fn fragments_in_id_order(
        &self,
    ) -> impl ExactSizeIterator<Item = (FragmentId, &NativePlanFragment)> + '_ {
        self.by_fragment
            .iter()
            .map(|(&fragment_id, fragment)| (fragment_id, fragment))
    }

    pub(crate) fn get(&self, fragment_id: FragmentId) -> Option<&NativePlanFragment> {
        self.by_fragment.get(&fragment_id)
    }

    pub(crate) fn into_fragments(self) -> btree_map::IntoIter<FragmentId, NativePlanFragment> {
        self.by_fragment.into_iter()
    }

    pub(crate) fn matches_provenance(&self, provenance: u64) -> bool {
        self.provenance == Some(provenance)
    }

    /// Bind the RF-specific payload after the generic plan encoder has frozen
    /// every non-RF fragment fact. This is deliberately consuming: a fragment
    /// shell cannot be reused to attach a second set of runtime-filter tables.
    pub(crate) fn bind_runtime_filter_tables(
        mut self,
        tables: BTreeMap<FragmentId, novarocks_protocol::plan::RuntimeFilterBindingTable>,
    ) -> Result<Self, String> {
        let expected = self.by_fragment.keys().copied().collect::<BTreeSet<_>>();
        let actual = tables.keys().copied().collect::<BTreeSet<_>>();
        if expected != actual {
            return Err(fragment_set_error(
                "runtime filter attachment",
                &expected,
                &actual,
            ));
        }
        for (fragment_id, fragment) in &mut self.by_fragment {
            let table = tables
                .get(fragment_id)
                .expect("validated runtime-filter table key set");
            if table.fragment_id != *fragment_id {
                return Err(format!(
                    "runtime filter attachment table fragment mismatch: key={fragment_id} table_fragment_id={}",
                    table.fragment_id
                ));
            }
            if fragment.runtime_filter_bindings.is_some() {
                return Err(format!(
                    "native fragment {fragment_id} already has runtime filter bindings"
                ));
            }
            fragment.runtime_filter_bindings = Some(table.clone());
        }
        Ok(self)
    }
}

#[cfg(feature = "query-execution-contract-test-support")]
pub(crate) fn native_fragment_bundle_for_contract_test(
    fragments: Vec<NativePlanFragment>,
) -> Result<NativeFragmentBundle, String> {
    let expected_ids = fragments
        .iter()
        .map(|fragment| fragment.fragment_id)
        .collect::<BTreeSet<_>>();
    collect_native_fragment_bundle(fragments, &expected_ids, None)
}

/// Encode one immutable distributed plan and its exact prepared bindings into
/// the native FE-to-BE wire bundle.
pub fn encode_native_fragment_bundle(
    source: NativeFragmentEncodingSource<'_>,
) -> Result<NativeFragmentBundle, String> {
    let NativeFragmentEncodingSource {
        plan,
        prepared,
        provenance,
    } = source;
    let encoded = super::plan::encode_distributed_plan_from_prepared(
        plan,
        prepared.scan_bindings(),
        prepared,
    )?;
    let sealed_ids = plan
        .fragments()
        .iter()
        .map(|fragment| fragment.fragment_id)
        .collect::<BTreeSet<_>>();
    let prepared_ids = prepared.fragment_ids();
    if prepared_ids != sealed_ids {
        return Err(fragment_set_error("prepared", &sealed_ids, &prepared_ids));
    }
    collect_native_fragment_bundle(encoded.fragments, &prepared_ids, provenance)
}

fn collect_native_fragment_bundle(
    fragments: impl IntoIterator<Item = NativePlanFragment>,
    expected_ids: &BTreeSet<FragmentId>,
    provenance: Option<u64>,
) -> Result<NativeFragmentBundle, String> {
    let mut by_fragment = BTreeMap::new();
    for fragment in fragments {
        let fragment_id = fragment.fragment_id;
        if by_fragment.insert(fragment_id, fragment).is_some() {
            return Err(format!(
                "native fragment bundle encoded duplicate fragment id={fragment_id}"
            ));
        }
    }
    let native_ids = by_fragment.keys().copied().collect::<BTreeSet<_>>();
    if native_ids != *expected_ids {
        return Err(fragment_set_error("native", expected_ids, &native_ids));
    }
    Ok(NativeFragmentBundle {
        by_fragment,
        provenance,
    })
}

fn fragment_set_error(
    label: &str,
    expected: &BTreeSet<FragmentId>,
    actual: &BTreeSet<FragmentId>,
) -> String {
    let missing = expected.difference(actual).copied().collect::<Vec<_>>();
    let unknown = actual.difference(expected).copied().collect::<Vec<_>>();
    format!(
        "{label} fragment ids mismatch: expected={expected:?} actual={actual:?} missing={missing:?} unknown={unknown:?}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
