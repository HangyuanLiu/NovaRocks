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

//! Extend the real ordered-source payload through the existing partition,
//! writer and receiver owners. The literal multiset is independent of hash
//! placement and decoding. Sender and receiver creation responsibilities stay
//! separate. The receiver projection carries its original metadata; its bare
//! copied payload Buffer is also readable after that projection exits.
use super::*;
use crate::exec::operators::data_stream_sink::partition_chunk_by_hash_arrays_created;
use crate::runtime::exchange::{
    ExchangeColumnBinding, ExchangePopResult, encode_chunks_with_creation,
};
use crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port;
use crate::runtime::fragment::io::{
    ExchangeReceiverFrame, ExchangeReceiverKey, ExchangeReceiverRegistration,
};
use novarocks_types::UniqueId;

pub(super) fn retain_received_buffer(
    input: &Chunk,
    layout: &novarocks_local_program::StaticLayout,
    sender: &dyn InputConstructionFactory,
    control: &RuntimeKernelControl,
) -> (
    crate::exec::chunk::ChunkBatchProjection,
    arrow::buffer::Buffer,
    Arc<MemTracker>,
) {
    let parts = partition_chunk_by_hash_arrays_created(
        input,
        &[input.columns()[0].clone()],
        3,
        false,
        sender,
        &|| control.checkpoint_execution(),
    )
    .unwrap()
    .into_iter()
    .filter(|c| !c.is_empty())
    .collect::<Vec<_>>();
    assert_eq!(parts.iter().map(Chunk::len).sum::<usize>(), 4);
    let payload = encode_chunks_with_creation(&parts, true, sender, control).unwrap();
    drop(parts);
    let tracker = MemTracker::new_root("complete-ordered-exchange-receiver");
    tracker.install_limit_once(64 << 20).unwrap();
    let runtime = state(tracker.clone());
    let factory = construction(&tracker, &runtime);
    let expected = factory.create_compiled_chunk_schema(layout).unwrap();
    assert_eq!(
        expected.slots()[1].typed_binding(),
        input.chunk_schema().slots()[1].typed_binding()
    );
    let port = in_process_test_exchange_receiver_port();
    let key = ExchangeReceiverKey {
        fragment_instance_id: UniqueId::new(0xc28, 2),
        node_id: 7,
    };
    port.register(ExchangeReceiverRegistration {
        key,
        expected_senders: 1,
        expected_chunk_schema: expected,
        column_binding: ExchangeColumnBinding::Positional,
        creation_factory: factory.clone(),
    })
    .unwrap();
    port.ensure_mem_tracker(key, &tracker).unwrap();
    let handle = port.receiver_handle(key, 1).unwrap();
    port.push(
        key,
        ExchangeReceiverFrame {
            source_fragment_instance_id: UniqueId::new(0xc28, 1),
            sender_ordinal: 0,
            sender_count: 1,
            sender_id: 0,
            backend_number: 0,
            sequence: 0,
            eos: true,
            payload,
        },
    )
    .unwrap();
    let mut actual = Vec::new();
    let mut retained = None;
    let mut projection = None;
    loop {
        match handle
            .try_pop_next_with_stats(1)
            .unwrap()
            .expect("synchronous delivered frame")
        {
            ExchangePopResult::Chunk(chunk) => {
                assert_eq!(
                    chunk.chunk_schema().slots()[0].typed_binding(),
                    input.chunk_schema().slots()[0].typed_binding()
                );
                assert_eq!(
                    chunk.chunk_schema().slots()[1].typed_binding(),
                    input.chunk_schema().slots()[1].typed_binding()
                );
                let keys = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = chunk.columns()[1]
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                for row in 0..chunk.len() {
                    let key = if keys.is_null(row) {
                        None
                    } else {
                        Some(keys.value(row).to_owned())
                    };
                    assert!(!values.is_null(row));
                    let bytes: [u8; 16] = values.value(row).try_into().unwrap();
                    actual.push((key, bytes));
                }
                retained = Some(chunk.columns()[1].to_data().buffers()[0].clone());
                projection = Some(chunk.into_batch_projection());
            }
            ExchangePopResult::Finished(stats) => {
                assert_eq!(stats.rows_received, 4);
                break;
            }
        }
    }
    actual.sort();
    let mut expected = vec![
        (None, 2_i128.to_be_bytes()),
        (None, 7_i128.to_be_bytes()),
        (Some("a".to_owned()), 1_i128.to_be_bytes()),
        (Some("a".to_owned()), 10_i128.to_be_bytes()),
    ];
    expected.sort();
    assert_eq!(actual, expected);
    let retained = retained.unwrap();
    let projection = projection.unwrap();
    port.remove(key);
    drop(handle);
    drop(port);
    drop(factory);
    drop(runtime);
    assert!(
        tracker.current() > 0,
        "the receiver typed projection retains its original construction stock"
    );
    (projection, retained, tracker)
}
