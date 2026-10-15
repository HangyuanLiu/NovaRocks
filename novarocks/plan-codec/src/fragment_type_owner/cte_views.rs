// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Batch borrowing of the actual producer-owned CTE partition/type namespace.
//! Numeric source indexes are prepared once; no type or partition is rebuilt.
use super::{
    ExecutionTypeEncodingPolicy, TypeEncodingResources, TypeEncodingResult, sort_ord_observed,
};
use crate::{
    PhysicalEncodeError,
    host_projection_v2::{AdmissionRefusal, ProjectionFailure},
    physical_encode::{PhysicalTypedCteConsumerView, find_sorted_key},
};
use novarocks_proto_models::plan;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
    ValueTypeStorageLifetime,
};
use std::convert::Infallible;

pub(crate) fn batch<'a>(
    encoded: &'a plan::DistributedPlan,
    control: &dyn PureCompileControl,
) -> Result<Vec<(usize, PhysicalTypedCteConsumerView<'a>)>, PhysicalEncodeError> {
    let mut request = |_: std::alloc::Layout| {
        control
            .checkpoint(CompilePhase::Validate, 0)
            .map_err(AdmissionRefusal::<Infallible>::Control)
    };
    let mut resources = TypeEncodingResources {
        request: &mut request,
        work: CompileCheckpoints::try_new(control, CompilePhase::Validate)?,
        type_policy: ExecutionTypeEncodingPolicy::PureGrammar,
        storage: ValueTypeStorageLifetime::pure(),
    };
    let result = prepare(encoded, &mut resources).map_err(ProjectionFailure::without_host);
    if matches!(&result, Err(PhysicalEncodeError::Control(_))) {
        return result;
    }
    resources.work.finish()?;
    result
}
fn prepare<'a, H>(
    encoded: &'a plan::DistributedPlan,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<Vec<(usize, PhysicalTypedCteConsumerView<'a>)>, H> {
    let mut count = 0usize;
    for edge in &encoded.edges {
        resources.work.step()?;
        if matches!(
            edge.edge_kind.as_ref().and_then(|kind| kind.kind.as_ref()),
            Some(plan::fragment_edge_kind::Kind::CteMulticast(_))
        ) {
            count = count
                .checked_add(1)
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
    }
    let mut views = resources.vector(count)?;
    if count == 0 {
        return Ok(views);
    }
    let mut fragments = resources.vector(encoded.fragments.len())?;
    let mut sinks = 0usize;
    for (ordinal, fragment) in encoded.fragments.iter().enumerate() {
        resources.work.step()?;
        fragments.push((fragment.fragment_id, ordinal));
        if let Some(plan::data_sink::Kind::MultiCastDataStream(multicast)) =
            fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        {
            sinks = sinks
                .checked_add(multicast.sinks.len())
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
    }
    sort_ord_observed(&mut fragments, &mut resources.work)?;
    for pair in fragments.windows(2) {
        resources.work.step()?;
        if pair[0].0 == pair[1].0 {
            return Err("CTE batch fragment identity is duplicated".into());
        }
    }
    let mut outputs = resources.vector(sinks)?;
    for (ordinal, fragment) in encoded.fragments.iter().enumerate() {
        resources.work.step()?;
        if let Some(plan::data_sink::Kind::MultiCastDataStream(multicast)) =
            fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        {
            for (sink_ordinal, sink) in multicast.sinks.iter().enumerate() {
                resources.work.step()?;
                outputs.push((
                    (
                        fragment.fragment_id,
                        sink.target_fragment_id,
                        sink.dest_node_id,
                    ),
                    (ordinal, sink_ordinal),
                ));
            }
        }
    }
    sort_ord_observed(&mut outputs, &mut resources.work)?;
    for pair in outputs.windows(2) {
        resources.work.step()?;
        if pair[0].0 == pair[1].0 {
            return Err("CTE batch consumer sink is duplicated".into());
        }
    }
    for (ordinal, edge) in encoded.edges.iter().enumerate() {
        resources.work.step()?;
        let Some(plan::fragment_edge_kind::Kind::CteMulticast(cte)) =
            edge.edge_kind.as_ref().and_then(|kind| kind.kind.as_ref())
        else {
            continue;
        };
        if cte.cte_id != edge.source_fragment_id {
            return Err("CTE edge disagrees with its producer identity".into());
        }
        let producer = find_sorted_key(&fragments, &edge.source_fragment_id, &mut resources.work)?
            .map(|position| &encoded.fragments[fragments[position].1])
            .ok_or("CTE producer fragment is absent")?;
        if producer.cte_id != Some(cte.cte_id) {
            return Err("CTE producer declaration disagrees with its edge".into());
        }
        let execution_types = producer
            .execution_types
            .as_ref()
            .ok_or("CTE producer has no execution type owner")?;
        let Some(plan::data_sink::Kind::MultiCastDataStream(multicast)) =
            producer.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            return Err("CTE producer has no multicast sink".into());
        };
        let key = (
            edge.source_fragment_id,
            edge.target_fragment_id,
            edge.target_exchange_node_id,
        );
        let position = find_sorted_key(&outputs, &key, &mut resources.work)?
            .ok_or("CTE consumer sink is absent")?;
        let (source_ordinal, sink_ordinal) = outputs[position].1;
        if !std::ptr::eq(producer, &encoded.fragments[source_ordinal]) {
            return Err("CTE consumer sink belongs to a different producer".into());
        }
        let output = &multicast.sinks[sink_ordinal];
        if output.output_columns.len() != edge.output_slot_ids.len()
            || cte.receive_producer_column_ids.len() != edge.output_slot_ids.len()
        {
            return Err("CTE consumer column count disagrees with its edge".into());
        }
        for ((sink_slot, edge_slot), receive) in output
            .output_columns
            .iter()
            .zip(&edge.output_slot_ids)
            .zip(&cte.receive_producer_column_ids)
        {
            resources.work.step()?;
            if sink_slot != edge_slot || u32::try_from(*edge_slot).ok() != Some(*receive) {
                return Err("CTE consumer column mapping disagrees with its producer".into());
            }
        }
        let output_partition = output
            .output_partition
            .as_ref()
            .ok_or("CTE consumer sink has no output partition")?;
        let kind = match plan::PartitionType::try_from(edge.output_partition) {
            Ok(plan::PartitionType::Unpartitioned) => plan::PartitionKind::Unpartitioned,
            Ok(plan::PartitionType::Random) => plan::PartitionKind::Random,
            Ok(plan::PartitionType::Hash | plan::PartitionType::BucketShuffleHash) => {
                plan::PartitionKind::Hash
            }
            _ => return Err("CTE edge has no supported partition kind".into()),
        };
        if output_partition.kind != kind as i32 {
            return Err("CTE consumer partition disagrees with its edge".into());
        }
        views.push((
            ordinal,
            PhysicalTypedCteConsumerView {
                cte_id: cte.cte_id,
                target_fragment_id: edge.target_fragment_id,
                target_exchange_node_id: edge.target_exchange_node_id,
                output_partition,
                execution_types,
                output_slot_ids: &edge.output_slot_ids,
                receive_producer_column_ids: &cte.receive_producer_column_ids,
            },
        ));
    }
    Ok(views)
}
