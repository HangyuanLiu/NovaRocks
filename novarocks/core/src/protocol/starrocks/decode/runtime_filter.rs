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

use std::collections::BTreeMap;

use crate::common::types::UniqueId;
use crate::protocol::common::error::FieldPath;
use crate::runtime::endpoint::RuntimeFilterProberDestination;
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
use crate::thrift::runtime_filter::{TRuntimeFilterParams, TRuntimeFilterProberParams};

use super::{StarRocksFragmentDecodeError, decode_runtime_endpoint};

pub(crate) fn decode_runtime_filter_params(
    source: &TRuntimeFilterParams,
    path: FieldPath,
) -> Result<RuntimeFilterParams, StarRocksFragmentDecodeError> {
    let id_to_prober_params = source
        .id_to_prober_params
        .as_ref()
        .map(|id_to_probers| {
            id_to_probers
                .iter()
                .map(|(filter_id, probers)| {
                    let destinations = probers
                        .iter()
                        .enumerate()
                        .map(|(index, prober)| {
                            decode_prober(
                                prober,
                                path.clone()
                                    .field("id_to_prober_params")
                                    .map_key(filter_id.to_string())
                                    .index(index),
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok((*filter_id, destinations))
                })
                .collect::<Result<BTreeMap<_, _>, StarRocksFragmentDecodeError>>()
        })
        .transpose()?
        .unwrap_or_default();
    let runtime_filter_builder_number = source
        .runtime_filter_builder_number
        .as_ref()
        .map(|counts| {
            counts
                .iter()
                .map(|(filter_id, count)| (*filter_id, *count))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    Ok(RuntimeFilterParams::new(
        id_to_prober_params,
        runtime_filter_builder_number,
        source.runtime_filter_max_size,
    ))
}

fn decode_prober(
    source: &TRuntimeFilterProberParams,
    path: FieldPath,
) -> Result<RuntimeFilterProberDestination, StarRocksFragmentDecodeError> {
    let fragment_instance_id = source.fragment_instance_id.as_ref().ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            path.clone().field("fragment_instance_id"),
            "runtime filter prober requires fragment_instance_id",
        )
    })?;
    let address = source.fragment_instance_address.as_ref().ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            path.clone().field("fragment_instance_address"),
            "runtime filter prober requires fragment_instance_address",
        )
    })?;
    let endpoint = decode_runtime_endpoint(address, path.field("fragment_instance_address"))?;
    Ok(RuntimeFilterProberDestination::new(
        UniqueId {
            hi: fragment_instance_id.hi,
            lo: fragment_instance_id.lo,
        },
        endpoint,
    ))
}
