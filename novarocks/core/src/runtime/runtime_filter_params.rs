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
#[cfg(feature = "compat")]
use crate::{common::types::UniqueId, runtime::endpoint::RuntimeEndpoint};

#[cfg(feature = "compat")]
use crate::thrift::{runtime_filter, types};

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

    #[cfg(feature = "compat")]
    pub(crate) fn from_thrift(src: &runtime_filter::TRuntimeFilterParams) -> Result<Self, String> {
        let id_to_prober_params = src
            .id_to_prober_params
            .as_ref()
            .map(|id_to_probers| {
                id_to_probers
                    .iter()
                    .map(|(filter_id, probers)| {
                        let destinations = probers
                            .iter()
                            .enumerate()
                            .map(|(idx, prober)| {
                                compat_adapters::prober_params_from_thrift(prober).map_err(|e| {
                                    format!("id_to_prober_params[{filter_id}][{idx}]: {e}")
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok((*filter_id, destinations))
                    })
                    .collect::<Result<BTreeMap<_, _>, String>>()
            })
            .transpose()?
            .unwrap_or_default();
        let runtime_filter_builder_number = src
            .runtime_filter_builder_number
            .as_ref()
            .map(|counts| {
                counts
                    .iter()
                    .map(|(filter_id, count)| (*filter_id, *count))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();

        Ok(Self::new(
            id_to_prober_params,
            runtime_filter_builder_number,
            src.runtime_filter_max_size,
        ))
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

#[cfg(feature = "compat")]
mod compat_adapters {
    use super::*;

    fn unique_id_from_thrift(src: &types::TUniqueId) -> UniqueId {
        UniqueId {
            hi: src.hi,
            lo: src.lo,
        }
    }

    pub(super) fn prober_params_from_thrift(
        src: &runtime_filter::TRuntimeFilterProberParams,
    ) -> Result<RuntimeFilterProberDestination, String> {
        let fragment_instance_id = src
            .fragment_instance_id
            .clone()
            .ok_or_else(|| "missing fragment_instance_id".to_string())?;
        let addr = src
            .fragment_instance_address
            .as_ref()
            .ok_or_else(|| "missing fragment_instance_address".to_string())?;
        let endpoint = RuntimeEndpoint::from_network_address(addr)?;
        Ok(RuntimeFilterProberDestination::new(
            unique_id_from_thrift(&fragment_instance_id),
            endpoint,
        ))
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
