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

use super::*;
use crate::exec::chunk::ChunkSchema;
use crate::exec::hash_table::tuple_partition::native_tuple_hashes;
use arrow::{
    array::{ArrayRef, BooleanArray, Int32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_type_contract::PartitionHashDomain;
fn chunk(keys: Option<Vec<Option<i32>>>, mask: Option<Vec<Option<bool>>>) -> Chunk {
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    let mut slots = Vec::new();
    if let Some(keys) = keys {
        fields.push(Field::new("key", DataType::Int32, true));
        arrays.push(Arc::new(Int32Array::from(keys)) as ArrayRef);
        slots.push(SlotId(1));
    }
    if let Some(mask) = mask {
        fields.push(Field::new("applies", DataType::Boolean, true));
        arrays.push(Arc::new(BooleanArray::from(mask)) as ArrayRef);
        slots.push(SlotId(2));
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
}
fn destinations(exchange: &LocalExchanger, chunk: &Chunk, cursor: &mut usize) -> Vec<usize> {
    let mut positions = vec![usize::MAX; chunk.len()];
    for (partition, rows) in exchange
        .driver_partition_rows(chunk, cursor)
        .unwrap()
        .iter()
        .enumerate()
    {
        for &row in rows {
            assert_eq!(positions[row as usize], usize::MAX);
            positions[row as usize] = partition;
        }
    }
    assert!(positions.iter().all(|v| *v != usize::MAX));
    positions
}
#[test]
fn native_tuple_domain_goldens_cover_four_by_four_four_by_two_and_three_by_four() {
    // Independently evaluated frozen FNV byte-order + full-avalanche vectors.
    // The expected pairs do not call the production helper under test.
    let vectors = [
        (
            4,
            4,
            vec![
                (3, 2),
                (1, 3),
                (2, 0),
                (3, 0),
                (0, 1),
                (1, 0),
                (1, 3),
                (0, 0),
                (2, 1),
                (0, 3),
                (3, 0),
                (1, 3),
                (3, 1),
                (3, 1),
                (0, 0),
                (3, 3),
            ],
        ),
        (
            4,
            2,
            vec![
                (3, 0),
                (1, 1),
                (2, 0),
                (3, 0),
                (0, 1),
                (1, 0),
                (1, 1),
                (0, 0),
                (2, 1),
                (0, 1),
                (3, 0),
                (1, 1),
                (3, 1),
                (3, 1),
                (0, 0),
                (3, 1),
            ],
        ),
        (
            3,
            4,
            vec![
                (0, 2),
                (2, 3),
                (1, 0),
                (0, 0),
                (2, 1),
                (0, 0),
                (2, 3),
                (0, 0),
                (2, 1),
                (1, 3),
                (2, 0),
                (1, 3),
                (0, 1),
                (1, 1),
                (1, 0),
                (0, 3),
            ],
        ),
    ];
    for (instances, drivers, expected) in vectors {
        let exchange = LocalExchanger::new_with_limits(
            drivers,
            2,
            LocalExchangePartitionSpec::HierarchicalInputSlotIds(vec![SlotId(1)]),
            1 << 20,
            -1,
        );
        let input = chunk(Some((0..16).map(Some).collect()), None);
        let actual = destinations(&exchange, &input, &mut 0);
        let hashes = native_tuple_hashes(&[input.columns()[0].clone()], 16).unwrap();
        for row in 0..16 {
            assert_eq!(
                (
                    (PartitionHashDomain::InstanceV1.mix(hashes[row]) % instances as u64) as usize,
                    actual[row]
                ),
                expected[row]
            );
        }
        // A fixed larger set reaches every driver within every instance. Each
        // key sent from another batch/producer still has one exact author.
        let input = chunk(Some((0..512).map(Some).collect()), None);
        let first = destinations(&exchange, &input, &mut 0);
        let mut cursor = drivers - 1;
        let second = destinations(&exchange, &input, &mut cursor);
        assert_eq!(first, second);
        assert_eq!(cursor, drivers - 1);
        let hashes = native_tuple_hashes(&[input.columns()[0].clone()], 512).unwrap();
        let mut seen = vec![vec![false; drivers]; instances];
        for (row, &driver) in first.iter().enumerate() {
            seen[(PartitionHashDomain::InstanceV1.mix(hashes[row]) % instances as u64) as usize]
                [driver] = true;
        }
        assert!(seen.iter().flatten().all(|v| *v));
    }
}
#[test]
fn bypass_round_robin_is_per_producer_and_continues_across_batches() {
    let exchange = LocalExchanger::new_with_limits(
        4,
        2,
        LocalExchangePartitionSpec::ConditionalKey {
            applies: SlotId(2),
            keys: vec![SlotId(1)],
        },
        1 << 20,
        -1,
    );
    let first = chunk(None, Some(vec![Some(false), None, Some(false)]));
    let second = chunk(None, Some(vec![None, Some(false), None]));
    let mut producer_a = 0;
    assert_eq!(
        destinations(&exchange, &first, &mut producer_a),
        vec![0, 1, 2]
    );
    assert_eq!(producer_a, 3);
    assert_eq!(
        destinations(&exchange, &second, &mut producer_a),
        vec![3, 0, 1]
    );
    assert_eq!(producer_a, 2);
    let mut producer_b = 0;
    assert_eq!(
        destinations(&exchange, &first, &mut producer_b),
        vec![0, 1, 2]
    );
    assert_eq!(producer_a, 2);
}
#[test]
fn true_rows_hash_the_real_target_and_false_null_rows_bypass_null_targets() {
    let exchange = LocalExchanger::new_with_limits(
        4,
        1,
        LocalExchangePartitionSpec::ConditionalKey {
            applies: SlotId(2),
            keys: vec![SlotId(1)],
        },
        1 << 20,
        -1,
    );
    let input = chunk(
        Some(vec![Some(7), None, Some(7), None]),
        Some(vec![Some(true), Some(false), Some(true), None]),
    );
    assert_eq!(destinations(&exchange, &input, &mut 0), vec![0, 0, 0, 1]);
    let input = chunk(Some(vec![None]), Some(vec![Some(true)]));
    assert!(exchange.driver_partition_rows(&input, &mut 0).is_err());
}
