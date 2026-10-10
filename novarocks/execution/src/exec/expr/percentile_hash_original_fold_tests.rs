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
//! Immutable original FE post-order folding evidence, separate from runtime demand.
use super::sql_fold_dependency_observation_tests::{REAL, compile_with_observer};
use arrow::array::{Array, StringArray};
use novarocks_sql::compiler::{
    FoldNodeKind, SqlConstantEvaluationError, SqlFoldDependencyInput, SqlFoldDependencyObserver,
    SqlFoldEvaluationOutcome,
};
use novarocks_type_contract::{CompileControlError, PureCompileControl};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Probe {
    ledger: Mutex<Vec<(bool, bool, bool)>>,
    hash_errors: Mutex<Vec<String>>,
}
impl SqlFoldDependencyObserver for Probe {
    fn before_fold_dependency_observed(
        &self,
        input: SqlFoldDependencyInput<'_>,
        _: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        let is_bad_cast = matches!(input.request.kind, FoldNodeKind::Cast(_))
            && input.request.args.first().is_some_and(|a| {
                a.value
                    .pool()
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .is_some_and(|strings| strings.value(a.value.ordinal() as usize) == "bad")
            });
        let is_hash =
            matches!(&input.request.kind,FoldNodeKind::Function{name} if name=="percentile_hash");
        if is_hash {
            assert_eq!(input.request.args.len(), 2);
        }
        self.ledger
            .lock()
            .unwrap()
            .push((is_bad_cast, is_hash, false));
        Ok(())
    }
    fn after_fold_dependency_observed(
        &self,
        input: SqlFoldDependencyInput<'_>,
        outcome: SqlFoldEvaluationOutcome<'_>,
    ) {
        let mut ledger = self.ledger.lock().unwrap();
        let receipt = ledger.last_mut().unwrap();
        match outcome {
            SqlFoldEvaluationOutcome::ProducedConstant(value) => {
                if receipt.0 {
                    assert!(value.pool().array().is_null(value.ordinal() as usize));
                }
                if receipt.1 {
                    panic!("original metadata must reject the complete two-argument HASH fold");
                }
                receipt.2 = true;
            }
            SqlFoldEvaluationOutcome::Error(SqlConstantEvaluationError::Evaluation(message)) => {
                if receipt.1 {
                    assert_eq!(message, "percentile_hash expects 1 to 1 arguments, got 2");
                    self.hash_errors.lock().unwrap().push(message.clone());
                }
                eprintln!(
                    "HASH original FE fold data refusal request={:?}: {message}",
                    input.request.kind
                );
            }
            other => eprintln!(
                "HASH original FE fold original outcome request={:?}: {other:?}",
                input.request.kind
            ),
        }
    }
}
#[test]
fn hash_original_real_fe_fold_evaluates_cast_tail_before_hash() {
    let probe = Arc::new(Probe::default());
    let source = compile_with_observer(
        "SELECT percentile_hash(CAST(17 AS INT),CAST('bad' AS INT)) AS encoded",
        &REAL,
        Some(probe.clone()),
    )
    .unwrap();
    let ledger = probe.ledger.lock().unwrap();
    eprintln!("HASH original FE post-order ledger={ledger:?}");
    let cast = ledger
        .iter()
        .position(|r| r.0)
        .expect("original FE evaluates bad CAST tail");
    let hash = ledger
        .iter()
        .position(|r| r.1)
        .expect("original FE then evaluates complete HASH source");
    assert!(cast < hash);
    assert!(ledger[cast].2);
    // The original post-order optimizer keeps HASH after the evaluator's
    // exact metadata refusal. It must not erase the logical tail argument.
    let hash_attempts = ledger.iter().filter(|r| r.1).count();
    assert!(hash_attempts > 0);
    assert!(ledger.iter().filter(|r| r.1).all(|r| !r.2));
    assert_eq!(probe.hash_errors.lock().unwrap().len(), hash_attempts);
    let mut retained = 0;
    for fragment in source.plan().fragments().values() {
        for (id, expression) in fragment.expressions().iter() {
            let novarocks_physical_plan::ExprKind::FunctionCall { function, args } =
                &expression.kind
            else {
                continue;
            };
            if function.function_id.as_str() != "builtin.scalar/percentile_hash/v1" {
                continue;
            }
            assert_eq!(args.len(), 2);
            assert_eq!(function.argument_types.len(), 2);
            let request = fragment
                .call_requests()
                .get(novarocks_physical_plan::PhysicalCallDefinition::Expression(
                    *id,
                ))
                .unwrap();
            assert_eq!(request.logical_argument_count, 2);
            assert_eq!(request.arguments.len(), 2);
            retained += 1;
        }
    }
    assert_eq!(retained, 1);
}
