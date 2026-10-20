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

//! Plain connector pages have typed construction ownership but no buffer
//! authority. These components exercise the source's original whole-batch
//! admission before ordered filtering, rather than prepaying the input fixture.
use super::*;
use crate::connector::SourcePageConverter;
use crate::exec::pipeline::operator::ConstructionFailure;
use novarocks_spi::connector::read_stack::SourcePage;

fn converted_plain_page(consumer: &Consumer) -> Chunk {
    // These are independent page allocations. Only the frozen declaration is
    // borrowed from the checked Scan; none of its prepaid input buffers is used.
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![
            Some("c"),
            Some("a"),
            Some("c"),
            None,
            Some("b"),
            None,
            Some("d"),
            Some("a"),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(30),
            Some(10),
            Some(3),
            Some(7),
            Some(20),
            Some(2),
            Some(40),
            Some(1),
        ])),
    ];
    let page = SourcePage::try_new(8, columns).unwrap();
    assert!(page.output_memory_bytes().is_none());
    let chunk = SourcePageConverter::with_output_schema(consumer.fixture.input.chunk_schema_ref())
        .convert_with_factory(page, consumer._creation.as_ref())
        .unwrap();
    assert!(!chunk.source_authority().is_attached());
    for (actual, prepaid) in chunk.columns().iter().zip(consumer.fixture.input.columns()) {
        assert!(!Arc::ptr_eq(actual, prepaid));
    }
    chunk
}

#[test]
fn compiled_ordered_page_source_plain_page_is_admitted_before_filter_and_limit() {
    let consumer = Consumer::new(Bind::Live);
    let query = LiteralQuery::new();
    consumer.script([rf::LivePollOutcome::Updated {
        snapshot: consumer.snapshot(1, query.clone()),
        terminal: Some(rf::LiveTerminal::Completed),
    }]);
    let page = converted_plain_page(&consumer);
    let original = Arc::downgrade(&page.columns()[1]);
    let mut source = SourceCase::new(consumer, Some(2));
    source.source.pages.push(page);
    source.bind();
    let mut output = source.pull().unwrap().unwrap();
    assert!(output.source_authority().is_attached());
    assert_eq!(output.len(), 2);
    assert_eq!(
        output.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("a"), None],
    );
    assert_eq!(
        output.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(10), Some(7)],
    );
    assert!(output.take_memory_lease().is_none());
    assert_eq!(query.calls.load(Ordering::SeqCst), 8);
    let effects = source.consumer.effects.0.lock().unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!((effects[0].input_rows(), effects[0].output_rows()), (8, 4));
    drop(effects);
    assert_eq!(source.source.claims.load(Ordering::SeqCst), 1);
    assert_eq!(source.source.pages.consumed.load(Ordering::SeqCst), 1);
    assert!(source.operator.is_finished());
    assert!(source.pull().unwrap().is_none());
    assert_eq!(source.source.pages.closes.load(Ordering::SeqCst), 1);
    assert!(source.operator.pending_finish().is_none());
    assert!(source.source.pages.operations.is_exited());
    let data = output.columns()[1].to_data();
    let last = data.buffers()[0].clone();
    drop(data);
    drop(output);
    let tracker = source.consumer.tracker.clone();
    drop(source);
    assert!(original.upgrade().is_some());
    assert!(tracker.current() > 0);
    std::thread::spawn(move || {
        drop(last);
        assert!(original.upgrade().is_none());
    })
    .join()
    .unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_ordered_page_source_first_whole_charge_refusal_precedes_live_queries() {
    let consumer = Consumer::new(Bind::Live);
    let query = LiteralQuery::new();
    consumer.script([rf::LivePollOutcome::Updated {
        snapshot: consumer.snapshot(1, query.clone()),
        terminal: Some(rf::LiveTerminal::Completed),
    }]);
    let page = converted_plain_page(&consumer);
    let original = Arc::downgrade(&page.columns()[1]);
    let bytes = i64::try_from(page.logical_bytes()).unwrap();
    assert!(bytes > 1);
    let mut source = SourceCase::new(consumer, None);
    // Binding creates the original source child tracker and live cursor before
    // imposing the positive limit on the first actual page buffer admission.
    source
        .operator
        .bind_runtime_state(&source.consumer.state)
        .unwrap();
    let current = source.consumer.tracker.current();
    assert!(current > 0);
    let limit = current.checked_add(bytes).unwrap().checked_sub(1).unwrap();
    source.consumer.tracker.install_limit_once(limit).unwrap();
    source.source.pages.push(page);
    let stop_observed = Arc::new(AtomicBool::new(false));
    let stopped = stop_observed.clone();
    let ticket = source
        .source
        .pages
        .operations
        .admit(Arc::new(move || {
            stopped.store(true, Ordering::SeqCst);
        }))
        .unwrap();
    let error = source.pull().unwrap_err();
    let ExecutionFailureCause::Construction(ConstructionFailure::Memory(cause)) = error.cause()
    else {
        panic!("expected original typed whole-page memory refusal: {error:?}");
    };
    assert!(!cause.detail().is_empty());
    assert_eq!(query.calls.load(Ordering::SeqCst), 0);
    assert!(source.consumer.effects.0.lock().unwrap().is_empty());
    assert!(
        source
            .consumer
            .session
            .cursor
            .observed
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(original.upgrade().is_none());
    assert_eq!(source.source.claims.load(Ordering::SeqCst), 1);
    assert_eq!(source.source.pages.consumed.load(Ordering::SeqCst), 0);
    source.operator.retire_data().unwrap();
    source.poll_exit();
    assert!(stop_observed.load(Ordering::SeqCst));
    assert!(matches!(
        source.operator.pending_finish(),
        Some(FinishWatch::Notify(_))
    ));
    ticket.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
    source.poll_exit();
    assert!(source.operator.pending_finish().is_none());
    source.finish();
}
