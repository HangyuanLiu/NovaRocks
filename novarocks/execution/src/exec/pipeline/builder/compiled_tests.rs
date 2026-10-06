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

use crate::exec::chunk::Chunk;
use crate::exec::expr::compiled_program::tests::{SEED_42_FIRST, SeedMode, program};
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::runtime::runtime_state::RuntimeState;
use arrow::array::{Float64Array, Int64Array};
use std::sync::Arc;
use std::time::Duration;

fn run(program: Arc<novarocks_local_program::LocalProgram>) -> Vec<Chunk> {
    let state = Arc::new(RuntimeState::new(
        None,
        None,
        None,
        None,
        None,
        None,
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ));
    let output = ResultSinkHandle::new();
    let prepared = prepare_compiled_program_pipeline_execution(
        program,
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        1,
        state,
        Arc::new(crate::runtime::fragment::io::NoopFragmentEventSink),
    )
    .expect("compiled program prepares drivers");
    prepared.start().join().expect("compiled program runs");
    output.take_chunks()
}

// Values -> Project(literals) -> Filter -> Project(RAND(seed), seed, seed) ->
// Limit -> Result, compiled by local-compiler and executed only through
// compiled roots. The RAND oracle is independent of the evaluator.
#[test]
fn compiled_program_runs_through_compiled_roots_to_result_rows() {
    for mode in [SeedMode::Input, SeedMode::DirectConstant] {
        let chunks = run(program(mode, false));
        let rows: usize = chunks.iter().map(Chunk::len).sum();
        assert_eq!(rows, 1);
        let batch = &chunks.iter().find(|c| c.len() == 1).unwrap().batch;
        let sample = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("RAND result is Float64");
        assert_eq!(sample.value(0).to_bits(), SEED_42_FIRST);
        for column in 1..3 {
            let seed = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("seed passthrough is Int64");
            assert_eq!(seed.value(0), 42);
        }
    }
}

// One RAND definition with two actual root uses: each root owns its own
// instance in the compiled Project, so both start from the seed's first value.
#[test]
fn twin_roots_of_one_definition_keep_independent_instances_in_the_pipeline() {
    let chunks = run(program(SeedMode::DirectConstant, true));
    let batch = &chunks.iter().find(|c| c.len() == 1).unwrap().batch;
    for column in 0..2 {
        let sample = batch
            .column(column)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("RAND result is Float64");
        assert_eq!(sample.value(0).to_bits(), SEED_42_FIRST, "root {column}");
    }
}
