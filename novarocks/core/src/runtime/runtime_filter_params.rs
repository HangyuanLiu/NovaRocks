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

use std::collections::{BTreeMap, HashMap};

use crate::runtime::endpoint::RuntimeFilterProberDestination;
use crate::runtime::runtime_filter_worker::{RuntimeFilterProberTarget, RuntimeFilterWorkerParams};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterParams {
    id_to_prober_params: BTreeMap<i32, Vec<RuntimeFilterProberDestination>>,
    runtime_filter_builder_number: BTreeMap<i32, i32>,
    runtime_filter_max_size: Option<i64>,
}

impl RuntimeFilterParams {
    pub(crate) fn new(
        id_to_prober_params: BTreeMap<i32, Vec<RuntimeFilterProberDestination>>,
        runtime_filter_builder_number: BTreeMap<i32, i32>,
        runtime_filter_max_size: Option<i64>,
    ) -> Self {
        Self {
            id_to_prober_params,
            runtime_filter_builder_number,
            runtime_filter_max_size: runtime_filter_max_size.filter(|size| *size > 0),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.id_to_prober_params.is_empty()
            && self.runtime_filter_builder_number.is_empty()
            && self.runtime_filter_max_size.is_none()
    }

    pub(crate) fn id_to_prober_params(
        &self,
    ) -> &BTreeMap<i32, Vec<RuntimeFilterProberDestination>> {
        &self.id_to_prober_params
    }

    pub(crate) fn runtime_filter_builder_number(&self) -> &BTreeMap<i32, i32> {
        &self.runtime_filter_builder_number
    }

    pub(crate) fn runtime_filter_max_size(&self) -> Option<i64> {
        self.runtime_filter_max_size
    }

    pub(crate) fn to_worker_params(&self) -> RuntimeFilterWorkerParams {
        let id_to_prober_targets = self
            .id_to_prober_params
            .iter()
            .map(|(filter_id, probers)| {
                (
                    *filter_id,
                    probers
                        .iter()
                        .map(|prober| {
                            RuntimeFilterProberTarget::new(
                                prober.endpoint().host().to_string(),
                                prober.endpoint().port(),
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let runtime_filter_builder_number = self
            .runtime_filter_builder_number
            .iter()
            .map(|(filter_id, count)| (*filter_id, *count))
            .collect::<HashMap<_, _>>();
        RuntimeFilterWorkerParams::new(
            id_to_prober_targets,
            runtime_filter_builder_number,
            self.runtime_filter_max_size,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::common::types::UniqueId;
    use crate::runtime::endpoint::{RuntimeEndpoint, RuntimeFilterProberDestination};

    use super::RuntimeFilterParams;

    fn destination(hi: i64, lo: i64, endpoint: &str) -> RuntimeFilterProberDestination {
        RuntimeFilterProberDestination::new(
            UniqueId { hi, lo },
            RuntimeEndpoint::parse(endpoint).expect("endpoint"),
        )
    }

    #[test]
    fn runtime_filter_worker_params_derive_from_native_destinations() {
        let params = RuntimeFilterParams::new(
            BTreeMap::from([(
                17,
                vec![
                    destination(5, 6, "10.0.0.17:8060"),
                    destination(7, 8, "10.0.0.18:8061"),
                ],
            )]),
            BTreeMap::from([(17, 4)]),
            Some(8192),
        );

        let worker = params.to_worker_params();
        let targets = worker.prober_targets(17).expect("targets");

        assert_eq!(worker.expected_builders(17), 4);
        assert_eq!(worker.runtime_filter_max_size(), Some(8192));
        assert_eq!(targets[0].hostname(), "10.0.0.17");
        assert_eq!(targets[0].port(), 8060);
        assert_eq!(targets[1].hostname(), "10.0.0.18");
        assert_eq!(targets[1].port(), 8061);
    }
}
