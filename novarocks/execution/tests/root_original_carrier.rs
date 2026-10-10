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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, DictionaryArray, ListArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Int32Type};
use arrow::record_batch::RecordBatch;
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks_execution::exec::pipeline::driver::{DriverState, PipelineDriver};
use novarocks_execution::exec::pipeline::operator::{Operator, ProcessorOperator};
use novarocks_execution::runtime::fragment::ExecutionResult;
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime::profile::{OperatorProfiles, Profiler};
use novarocks_execution::runtime::runtime_state::RuntimeState;
use novarocks_types::SlotId;
use novarocks_types::logical::{LogicalType, field_with_logical_type};

type Received = Arc<Mutex<Option<Chunk>>>;

struct OneChunkSource {
    chunk: Option<Chunk>,
}

impl Operator for OneChunkSource {
    fn name(&self) -> &str {
        "OriginalCarrierSource"
    }

    fn is_finished(&self) -> bool {
        self.chunk.is_none()
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for OneChunkSource {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        self.chunk.is_some()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> ExecutionResult<()> {
        Err("the source does not accept input".to_string().into())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        Ok(self.chunk.take())
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        Ok(())
    }
}

struct OriginalReceiver {
    received: Received,
    finished: bool,
    arrays: Vec<ArrayRef>,
    schema: ChunkSchemaRef,
    source_tracker: Arc<MemTracker>,
    original_charge: i64,
}

struct OrdinaryReceiver {
    received: Received,
    finished: bool,
}

macro_rules! receiver_operator {
    ($receiver:ty, $name:literal) => {
        impl Operator for $receiver {
            fn name(&self) -> &str {
                $name
            }

            fn is_finished(&self) -> bool {
                self.finished
            }

            fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
                Some(self)
            }

            fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
                Some(self)
            }
        }
    };
}

receiver_operator!(OriginalReceiver, "OriginalReceiver");
receiver_operator!(OrdinaryReceiver, "OrdinaryReceiver");

macro_rules! receiver_processor_readiness {
    () => {
        fn need_input(&self) -> bool {
            !self.finished && self.received.lock().unwrap().is_none()
        }

        fn has_output(&self) -> bool {
            false
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            self.finished = true;
            Ok(())
        }
    };
}

impl ProcessorOperator for OriginalReceiver {
    receiver_processor_readiness!();

    fn takes_original_input(&self) -> bool {
        true
    }

    fn accepts_encoded_column(&self, _slot: SlotId, _data_type: &DataType) -> bool {
        panic!("original input must bypass dictionary hydration and profiler inspection")
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        assert!(Arc::ptr_eq(&self.schema, &chunk.chunk_schema_ref()));
        assert_eq!(chunk.columns().len(), self.arrays.len());
        for (actual, original) in chunk.columns().iter().zip(&self.arrays) {
            assert!(
                Arc::ptr_eq(actual, original),
                "the carrier was reconstructed"
            );
            assert_eq!(actual.data_type(), original.data_type());
        }
        assert_eq!(
            chunk.columns()[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(
            self.source_tracker.current(),
            self.original_charge,
            "the original input accounting owner must survive the final edge"
        );
        *self.received.lock().unwrap() = Some(chunk);
        Ok(())
    }
}

impl ProcessorOperator for OrdinaryReceiver {
    receiver_processor_readiness!();

    // Keep the default takes_original_input/accepts_encoded_column behavior.
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        assert_eq!(chunk.columns()[0].data_type(), &DataType::Utf8);
        *self.received.lock().unwrap() = Some(chunk);
        Ok(())
    }
}

fn source_chunk() -> Chunk {
    let dictionary: ArrayRef = Arc::new(
        [Some("alpha"), None, Some("omega")]
            .into_iter()
            .collect::<DictionaryArray<Int32Type>>(),
    );
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(7), None]),
        None,
        Some(vec![Some(11)]),
    ]));
    let json_field = field_with_logical_type(
        Field::new("document", DataType::Utf8, false).with_metadata(HashMap::from([(
            "nested_origin".to_string(),
            "catalog-json-field".to_string(),
        )])),
        LogicalType::Json,
    );
    let nested_fields = Fields::from(vec![
        Field::new("items", list.data_type().clone(), true),
        json_field,
    ]);
    let nested: ArrayRef = Arc::new(
        StructArray::try_new(
            nested_fields,
            vec![
                list,
                Arc::new(StringArray::from(vec!["{}", "{\"x\":1}", "[]"])),
            ],
            None,
        )
        .unwrap(),
    );
    let arrays = vec![dictionary, nested];
    let fields = [
        Field::new("encoded", arrays[0].data_type().clone(), true).with_metadata(HashMap::from([
            ("carrier_origin".to_string(), "original".to_string()),
        ])),
        Field::new("nested", arrays[1].data_type().clone(), false).with_metadata(HashMap::from([
            ("nested_contract".to_string(), "preserved".to_string()),
        ])),
    ];
    let slots = fields
        .into_iter()
        .enumerate()
        .map(|(ordinal, field)| {
            ChunkSlotSchema::new_with_field(
                SlotId::new(ordinal as u32 + 1),
                field,
                None,
                Some(ordinal as i32 + 101),
            )
        })
        .collect();
    let schema = Arc::new(
        ChunkSchema::try_new_with_schema_metadata(
            slots,
            HashMap::from([("source_owner".to_string(), "original-fixture".to_string())]),
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_new(schema.arrow_schema_ref(), arrays).unwrap();
    Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
}

fn run_driver(chunk: Chunk, sink: Box<dyn Operator>) -> OperatorProfiles {
    let profiler = Profiler::new("root-original-carrier");
    let profiles = vec![
        OperatorProfiles::new(profiler.child("Source")),
        OperatorProfiles::new(profiler.child("Receiver")),
    ];
    let sink_profile = profiles[1].clone();
    let runtime = Arc::new(RuntimeState::new(
        None,
        None,
        None,
        None,
        None,
        Some(MemTracker::new_root("driver-accounting")),
        None,
    ));
    let mut driver = PipelineDriver::new(
        0,
        vec![Box::new(OneChunkSource { chunk: Some(chunk) }), sink],
        Some(profiler),
        profiles,
        runtime,
        None,
    );
    for _ in 0..16 {
        match driver.process(Duration::from_millis(20)) {
            DriverState::Finished => return sink_profile,
            DriverState::Ready | DriverState::Running => {}
            DriverState::Failed(error) => panic!("driver failed: {error}"),
            other => panic!("driver did not finish its finite input: {other:?}"),
        }
    }
    panic!("driver failed to finish within the bounded turn count")
}

#[test]
fn original_receiver_preserves_carrier_schema_and_accounting_with_profiler() {
    let mut chunk = source_chunk();
    let arrays = chunk.columns().to_vec();
    let schema = chunk.chunk_schema_ref();
    let source_tracker = MemTracker::new_root("precovered-original-input");
    chunk.transfer_to(&source_tracker);
    let original_charge = source_tracker.current();
    assert!(original_charge > 0);
    let received = Arc::new(Mutex::new(None));
    let sink = OriginalReceiver {
        received: Arc::clone(&received),
        finished: false,
        arrays,
        schema: Arc::clone(&schema),
        source_tracker: Arc::clone(&source_tracker),
        original_charge,
    };
    let profiles = run_driver(chunk, Box::new(sink));
    let chunk = received.lock().unwrap().take().unwrap();
    assert!(Arc::ptr_eq(&schema, &chunk.chunk_schema_ref()));
    assert_eq!(profiles.unique.counter_value("DictInputColumns"), Some(0));
    assert_eq!(
        profiles.unique.counter_value("DictHydratedColumns"),
        Some(0)
    );
    assert_eq!(
        profiles.common.counter_value("OperatorPeakMemoryUsage"),
        Some(0)
    );
    assert_eq!(source_tracker.current(), original_charge);
    drop(chunk);
    assert_eq!(source_tracker.current(), 0);
}

#[test]
fn ordinary_receiver_still_hydrates_dictionary_and_records_profile() {
    let chunk = source_chunk();
    let original_array = Arc::clone(&chunk.columns()[0]);
    let metadata = chunk.schema().metadata().clone();
    let received = Arc::new(Mutex::new(None));
    let sink = OrdinaryReceiver {
        received: Arc::clone(&received),
        finished: false,
    };
    assert!(!sink.takes_original_input());
    let profiles = run_driver(chunk, Box::new(sink));
    let chunk = received.lock().unwrap().take().unwrap();
    assert!(!Arc::ptr_eq(&original_array, &chunk.columns()[0]));
    assert_eq!(chunk.schema().metadata(), &metadata);
    let values = chunk.columns()[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "alpha");
    assert!(values.is_null(1));
    assert_eq!(values.value(2), "omega");
    assert_eq!(profiles.unique.counter_value("DictInputColumns"), Some(1));
    assert_eq!(
        profiles.unique.counter_value("DictHydratedColumns"),
        Some(1)
    );
    assert_eq!(profiles.unique.counter_value("DictHydratedRows"), Some(3));
}
