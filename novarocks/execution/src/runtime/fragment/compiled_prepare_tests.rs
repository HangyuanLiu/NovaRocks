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

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arrow::array::{Float64Array, Int64Array};
use novarocks_types::{QueryId, UniqueId};

use super::*;
use crate::exec::chunk::Chunk;
use crate::exec::expr::compiled_program::tests::{SEED_42_FIRST, SeedMode, program};
use crate::exec::fragment::program::FragmentContractVersion;
use crate::runtime::fragment::instance::{
    BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
    FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
};
use crate::runtime::fragment::io::{
    FragmentIoError, FragmentResultSession, FragmentResultWriter, ResultAbort,
    ResultWriteAdmission, ResultWriteCredit, ResultWriteSpec,
};
use crate::runtime::observable::Observable;
use crate::runtime::query_options::QueryOptions;

#[derive(Default)]
struct CollectingSession {
    chunks: Mutex<Vec<Chunk>>,
    finished: Mutex<bool>,
}

impl FragmentResultSession for CollectingSession {
    fn reservation_bytes(&self, chunk: &Chunk) -> Result<usize, FragmentIoError> {
        Ok(chunk.logical_bytes())
    }

    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, FragmentIoError> {
        Ok(ResultWriteAdmission::Granted(ResultWriteCredit::new(
            bytes,
            |_| {},
        )))
    }

    fn writable_observable(&self) -> Option<Arc<Observable>> {
        None
    }

    fn write_with_credit(
        &self,
        chunk: Chunk,
        _credit: ResultWriteCredit,
    ) -> Result<(), FragmentIoError> {
        self.chunks.lock().unwrap().push(chunk);
        Ok(())
    }

    fn finish(&self) -> Result<(), FragmentIoError> {
        *self.finished.lock().unwrap() = true;
        Ok(())
    }

    fn abort(&self, _reason: ResultAbort) {}
}

struct CollectingWriter {
    session: Arc<CollectingSession>,
}

impl FragmentResultWriter for CollectingWriter {
    fn open(
        &self,
        _spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
        Ok(self.session.clone())
    }
}

fn refused(result: Result<DormantFragmentHandle, FragmentLaunchError>) -> FragmentLaunchError {
    match result {
        Ok(_) => panic!("compiled preparation must be refused"),
        Err(error) => error,
    }
}

fn instance(
    finst_id: UniqueId,
    pipeline_dop: usize,
    exchange_inputs: ExchangeInputAssignments,
) -> FragmentInstanceSpec {
    FragmentInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        QueryId::new(finst_id.high() - 2, finst_id.low() - 2),
        FragmentInstanceId::new(finst_id),
        ScanAssignments::default(),
        exchange_inputs,
        FragmentSinkAssignment::None,
        FragmentRuntimeOptions::new(QueryOptions::default(), false),
        NonZeroUsize::new(pipeline_dop).expect("nonzero DOP"),
        BackendNum::try_new(1).expect("backend number"),
    )
}

// The Task-facing entry prepares a compiled program into the ordinary dormant
// handle: its result rows reach the opened result session, computed only by
// compiled roots.
#[test]
fn compiled_submission_prepares_and_runs_into_the_result_session() {
    let program = program(SeedMode::Input, false);
    let submission = CompiledFragmentSubmission::try_new(
        program,
        instance(
            UniqueId::new(201, 202),
            1,
            ExchangeInputAssignments::default(),
        ),
    )
    .expect("compiled submission");
    assert_eq!(submission.sink_kind(), FragmentSinkKind::Result);
    let session = Arc::new(CollectingSession::default());
    let mut context = FragmentPrepareContext::default();
    context.result_writer = Arc::new(CollectingWriter {
        session: Arc::clone(&session),
    });
    let handle = prepare_compiled_fragment(submission, context).expect("compiled prepare");
    assert!(matches!(
        handle.start().join().outcome(),
        FragmentOutcome::Succeeded
    ));
    let chunks = session.chunks.lock().unwrap();
    let rows: usize = chunks.iter().map(Chunk::len).sum();
    assert_eq!(rows, 1);
    let batch = &chunks.iter().find(|chunk| chunk.len() == 1).unwrap().batch;
    let sample = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("RAND result is Float64");
    assert_eq!(sample.value(0).to_bits(), SEED_42_FIRST);
    let seed = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("seed passthrough is Int64");
    assert_eq!(seed.value(0), 42);
}

#[test]
fn compiled_submission_refuses_a_foreign_dop_and_unaddressed_inputs() {
    let program = program(SeedMode::Input, false);
    let error = CompiledFragmentSubmission::try_new(
        Arc::clone(&program),
        instance(
            UniqueId::new(203, 204),
            2,
            ExchangeInputAssignments::default(),
        ),
    )
    .expect_err("compiled DOP is a frozen profile fact");
    assert!(error.to_string().contains("pipeline DOP"), "{error}");

    // An exchange assignment for a receiver the program does not have is
    // refused at registration, and nothing stays registered.
    let stray = ExchangeInputAssignments::new(std::collections::BTreeMap::from([(
        crate::exec::fragment::program::FragmentNodeId::new(7),
        ExchangeInputAssignment::new(NonZeroUsize::new(1).unwrap()),
    )]));
    let submission =
        CompiledFragmentSubmission::try_new(program, instance(UniqueId::new(205, 206), 1, stray))
            .expect("submission facts are consistent");
    let error = refused(prepare_compiled_fragment(
        submission,
        FragmentPrepareContext::default(),
    ));
    assert!(
        error
            .to_string()
            .contains("has no compiled exchange source"),
        "{error}"
    );
}

#[test]
fn a_host_root_sink_width_must_equal_the_compiled_width() {
    let program = program(SeedMode::Input, false);
    let submission = CompiledFragmentSubmission::try_new(
        program,
        instance(
            UniqueId::new(207, 208),
            1,
            ExchangeInputAssignments::default(),
        ),
    )
    .expect("compiled submission");
    let mut context = FragmentPrepareContext::default();
    context.root_sink_dop = Some(4);
    let error = refused(prepare_compiled_fragment(submission, context));
    assert!(error.to_string().contains("root sink width"), "{error}");
}
