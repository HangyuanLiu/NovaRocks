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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

use crate::common::types::UniqueId;
use crate::runtime::profile::RuntimeProfileTree;

#[derive(Default)]
struct StandaloneQueryProfileRegistry {
    active: BTreeSet<(i64, i64)>,
    profiles: BTreeMap<(i64, i64), BTreeMap<(i64, i64), RuntimeProfileTree>>,
}

fn standalone_query_profiles() -> &'static Mutex<StandaloneQueryProfileRegistry> {
    static REGISTRY: OnceLock<Mutex<StandaloneQueryProfileRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(StandaloneQueryProfileRegistry::default()))
}

fn query_profile_key(query_id: &UniqueId) -> (i64, i64) {
    (query_id.hi, query_id.lo)
}

pub(crate) fn record_native_standalone_query_profile_report(
    report: &crate::proto::novarocks::ExecStatusReport,
) -> Result<bool, String> {
    let Some(query_id) = report.query_id.as_ref() else {
        return Ok(false);
    };
    let key = (query_id.hi, query_id.lo);
    let mut guard = standalone_query_profiles()
        .lock()
        .expect("standalone query profile registry lock");
    if !guard.active.contains(&key) {
        return Ok(false);
    }

    let Some(status) = report.status.as_ref() else {
        return Err("ExecStatusReport missing status".to_string());
    };
    if report.done
        && status.code == 0
        && let Some(profile) = report.profile.as_ref()
    {
        let Some(finst_id) = report.fragment_instance_id.as_ref() else {
            return Err("ExecStatusReport missing fragment_instance_id".to_string());
        };
        let native = RuntimeProfileTree::from_proto(profile)?;
        guard
            .profiles
            .entry(key)
            .or_default()
            .insert((finst_id.hi, finst_id.lo), native);
    }
    Ok(true)
}

pub(crate) fn standalone_query_profile_count(query_id: &UniqueId) -> usize {
    standalone_query_profiles()
        .lock()
        .expect("standalone query profile registry lock")
        .profiles
        .get(&query_profile_key(query_id))
        .map(BTreeMap::len)
        .unwrap_or(0)
}

pub(crate) fn take_standalone_query_profiles(
    query_id: &UniqueId,
) -> BTreeMap<UniqueId, RuntimeProfileTree> {
    standalone_query_profiles()
        .lock()
        .expect("standalone query profile registry lock")
        .profiles
        .remove(&query_profile_key(query_id))
        .map(|profiles| {
            profiles
                .into_iter()
                .map(|((hi, lo), profile)| (UniqueId { hi, lo }, profile))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) struct StandaloneQueryProfileGuard {
    key: (i64, i64),
}

impl StandaloneQueryProfileGuard {
    pub(crate) fn register(query_id: &UniqueId) -> Self {
        let key = query_profile_key(query_id);
        let mut guard = standalone_query_profiles()
            .lock()
            .expect("standalone query profile registry lock");
        guard.profiles.remove(&key);
        guard.active.insert(key);
        Self { key }
    }
}

impl Drop for StandaloneQueryProfileGuard {
    fn drop(&mut self) {
        let mut guard = standalone_query_profiles()
            .lock()
            .expect("standalone query profile registry lock");
        guard.active.remove(&self.key);
        guard.profiles.remove(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native_report(
        query_id: UniqueId,
        finst_id: Option<UniqueId>,
        status: Option<crate::proto::common::Status>,
        done: bool,
        node_id: Option<i32>,
    ) -> crate::proto::novarocks::ExecStatusReport {
        crate::proto::novarocks::ExecStatusReport {
            query_id: Some(crate::proto::common::UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
            fragment_instance_id: finst_id.map(|id| crate::proto::common::UniqueId {
                hi: id.hi,
                lo: id.lo,
            }),
            status,
            done,
            profile: node_id.map(|node_id| crate::proto::novarocks::RuntimeProfileTree {
                root: Some(crate::proto::novarocks::ProfileNode {
                    name: "root".to_string(),
                    node_id,
                    ..Default::default()
                }),
            }),
            ..Default::default()
        }
    }

    fn ok_status() -> crate::proto::common::Status {
        crate::proto::common::Status {
            code: 0,
            ..Default::default()
        }
    }

    #[test]
    fn active_registry_validates_status_and_fragment_instance_id() {
        let query_id = UniqueId { hi: 510_001, lo: 1 };
        let _guard = StandaloneQueryProfileGuard::register(&query_id);

        let missing_status = native_report(query_id, None, None, true, Some(1));
        assert_eq!(
            record_native_standalone_query_profile_report(&missing_status),
            Err("ExecStatusReport missing status".to_string())
        );

        let missing_finst = native_report(query_id, None, Some(ok_status()), true, Some(1));
        assert_eq!(
            record_native_standalone_query_profile_report(&missing_finst),
            Err("ExecStatusReport missing fragment_instance_id".to_string())
        );
    }

    #[test]
    fn inactive_or_incomplete_reports_do_not_add_profiles() {
        let inactive_query_id = UniqueId { hi: 510_002, lo: 1 };
        let finst_id = UniqueId { hi: 510_002, lo: 2 };
        let inactive = native_report(
            inactive_query_id,
            Some(finst_id),
            Some(ok_status()),
            true,
            Some(1),
        );
        assert_eq!(
            record_native_standalone_query_profile_report(&inactive),
            Ok(false)
        );

        let query_id = UniqueId { hi: 510_003, lo: 1 };
        let _guard = StandaloneQueryProfileGuard::register(&query_id);
        let incomplete = native_report(query_id, Some(finst_id), Some(ok_status()), false, Some(1));
        assert_eq!(
            record_native_standalone_query_profile_report(&incomplete),
            Ok(true)
        );
        assert_eq!(standalone_query_profile_count(&query_id), 0);
    }

    #[test]
    fn duplicate_fragment_instance_report_overwrites_profile() {
        let query_id = UniqueId { hi: 510_004, lo: 1 };
        let finst_id = UniqueId { hi: 510_004, lo: 2 };
        let _guard = StandaloneQueryProfileGuard::register(&query_id);

        for node_id in [7, 9] {
            let report = native_report(
                query_id,
                Some(finst_id),
                Some(ok_status()),
                true,
                Some(node_id),
            );
            assert!(record_native_standalone_query_profile_report(&report).unwrap());
        }

        assert_eq!(standalone_query_profile_count(&query_id), 1);
        let profiles = take_standalone_query_profiles(&query_id);
        assert_eq!(profiles.len(), 1);
        assert_eq!(
            profiles
                .get(&finst_id)
                .expect("canonical final profile remains keyed by finst_id")
                .root
                .node_id,
            9
        );
    }

    #[test]
    fn guard_drop_clears_profile_state() {
        let query_id = UniqueId { hi: 510_005, lo: 1 };
        let finst_id = UniqueId { hi: 510_005, lo: 2 };
        {
            let _guard = StandaloneQueryProfileGuard::register(&query_id);
            let report = native_report(query_id, Some(finst_id), Some(ok_status()), true, Some(3));
            assert!(record_native_standalone_query_profile_report(&report).unwrap());
            assert_eq!(standalone_query_profile_count(&query_id), 1);
        }

        assert_eq!(standalone_query_profile_count(&query_id), 0);
    }
}
