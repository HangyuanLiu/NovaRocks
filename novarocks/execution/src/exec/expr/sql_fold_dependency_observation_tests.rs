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
//! Real SQL completion with the original legacy arena as the folding calculator.
//! This is a bounded test adapter, not a copy of FE admission or a native receipt.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::lookup_function;
use crate::exec::expr::{ExprArena, ExprNode};
use arrow::array::{Array, RecordBatch, RecordBatchOptions, StringArray};
use novarocks_functions::{
    ConstantError, FunctionArgumentType, FunctionResultType, ResolvedFunctionBinding,
};
use novarocks_functions::{ConstantPool, ConstantValue};
use novarocks_physical_plan::{ExprKind, PipelineDopDomain, PlanVersionId, ScanReadBudget};
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, FoldArg, FoldNodeKind, FoldRequest, SessionOptimizerSettings,
    SqlAuthoredPhysicalPlan, SqlCompileControl, SqlCompileIntent, SqlCompileProgress, SqlCompiler,
    SqlConstantEvaluationError, SqlConstantEvaluator, SqlFinalPlanCompileRequest,
    SqlFoldDependencyInput, SqlFoldDependencyObserver, SqlFoldDependencySource,
    SqlFoldEvaluationOutcome, SqlPhysicalEmissionMode, SqlPlanningEnvironment, SqlSessionContext,
    SqlStatementInput, builtin_sql_function_catalog,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::sync::{Arc, LazyLock, Mutex};

#[derive(Clone)]
enum Mode {
    LegacyKernel,
    Decline,
    Fail(SqlConstantEvaluationError),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorTag {
    Evaluation,
    Control,
    Constant,
    InvalidType,
    Preparation,
}
#[derive(Debug)]
enum Outcome {
    Produced(ConstantValue),
    Declined,
    Error {
        tag: ErrorTag,
        raw_text_len: usize,
        control: Option<CompileControlError>,
    },
}
#[derive(Debug)]
struct Receipt {
    binding: Option<ResolvedFunctionBinding>,
    source_result_constraint: Option<FunctionValueType>,
    source_constraint_origin: Option<novarocks_sql::binding::SqlResultConstraintOrigin>,
    source_decimal_policy: Option<novarocks_type_contract::DecimalOverflowPolicy>,
    request_result: FunctionValueType,
    request_kind: FoldNodeKind,
    arguments: Vec<FoldArg>,
    outcome: Option<Outcome>,
}
struct Probe {
    refuse_start: Option<CompileControlError>,
    receipts: Mutex<Vec<Receipt>>,
}
impl Probe {
    fn new() -> Self {
        Self {
            refuse_start: None,
            receipts: Mutex::new(Vec::new()),
        }
    }
}
// Same native FE calculator route for these real single-node fixtures: original
// Constant -> ExprArena -> lookup_function execution dispatch -> original kernel.
// It performs no SQL binding/selection; observation receives the original binder.
fn calculate(
    mode: &Mode,
    request: &FoldRequest,
    control: &dyn PureCompileControl,
) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
    match mode {
        Mode::Decline => return Ok(None),
        Mode::Fail(error) => return Err(error.clone()),
        Mode::LegacyKernel => {}
    }
    let mut arena = ExprArena::default();
    let args = request
        .args
        .iter()
        .map(|argument| {
            assert_eq!(&argument.value_type, argument.value.value_type());
            arena.push_typed(
                ExprNode::Constant(argument.value.clone()),
                argument.value_type.data_type.clone(),
            )
        })
        .collect::<Vec<_>>();
    let node = match &request.kind {
        FoldNodeKind::Function { name } => {
            let Some(kind) = lookup_function(name) else {
                return Ok(None);
            };
            ExprNode::FunctionCall { kind, args }
        }
        FoldNodeKind::Cast(policy) if args.len() == 1 => ExprNode::Cast(args[0], *policy),
        _ => return Ok(None),
    };
    let root = arena.push_typed(node, request.result_type.data_type.clone());
    let schema = Arc::new(ChunkSchema::empty());
    let batch = RecordBatch::try_new_with_options(
        schema.arrow_schema_ref(),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    let output = arena.eval(root, &chunk)?;
    assert_eq!(output.len(), 1);
    assert_eq!(output.data_type(), &request.result_type.data_type);
    if output.is_null(0) && !request.result_type.nullable {
        return Ok(None);
    }
    let field = Arc::new(request.result_type.try_to_field("literal")?);
    let pool = ConstantPool::try_new(
        field,
        request.result_type.clone(),
        output.to_data(),
        request.constant_policy,
        CompilePhase::FunctionSpecialization,
        control,
    )?;
    Ok(Some(pool.value(0)?))
}
impl SqlFoldDependencyObserver for Probe {
    fn before_fold_dependency_observed(
        &self,
        input: SqlFoldDependencyInput<'_>,
        _: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        if let Some(cause) = self.refuse_start {
            return Err(cause);
        }
        let (source_result_constraint, source_constraint_origin, source_decimal_policy) =
            match input.source {
                SqlFoldDependencySource::Function(binding) => (
                    binding.result_constraint().cloned(),
                    Some(binding.result_constraint_origin().clone()),
                    Some(binding.decimal_overflow_policy()),
                ),
                SqlFoldDependencySource::Intrinsic => (None, None, None),
            };
        let binding = match input.source {
            SqlFoldDependencySource::Function(binding) => {
                assert!(matches!(input.request.kind, FoldNodeKind::Function { .. }));
                assert_eq!(binding.logical_argument_count, input.request.args.len());
                assert_eq!(
                    binding.selected.result_type,
                    FunctionResultType::Scalar(input.request.result_type.clone())
                );
                assert_eq!(
                    binding.selected.argument_types.len(),
                    input.request.args.len()
                );
                for (actual, argument) in binding
                    .selected
                    .argument_types
                    .iter()
                    .zip(&input.request.args)
                {
                    assert_eq!(
                        actual,
                        &FunctionArgumentType::Value(argument.value_type.clone())
                    );
                    assert_eq!(argument.value.value_type(), &argument.value_type);
                }
                Some(binding.resolved().clone())
            }
            SqlFoldDependencySource::Intrinsic => {
                assert!(!matches!(input.request.kind, FoldNodeKind::Function { .. }));
                None
            }
        };
        // Tests own an explicit bounded ledger. No formal MEM grant is claimed.
        let mut receipts = self.receipts.lock().unwrap();
        receipts
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        receipts.push(Receipt {
            binding,
            source_result_constraint,
            source_constraint_origin,
            source_decimal_policy,
            request_result: input.request.result_type.clone(),
            request_kind: input.request.kind.clone(),
            arguments: input.request.args.clone(),
            outcome: None,
        });
        Ok(())
    }
    fn after_fold_dependency_observed(
        &self,
        _: SqlFoldDependencyInput<'_>,
        outcome: SqlFoldEvaluationOutcome<'_>,
    ) {
        let outcome = match outcome {
            SqlFoldEvaluationOutcome::ProducedConstant(value) => Outcome::Produced(value.clone()),
            SqlFoldEvaluationOutcome::Declined => Outcome::Declined,
            SqlFoldEvaluationOutcome::Error(error) => {
                let (tag, raw_text_len, control) = match error {
                    SqlConstantEvaluationError::Evaluation(message) => {
                        (ErrorTag::Evaluation, message.len(), None)
                    }
                    SqlConstantEvaluationError::Control(cause) => {
                        (ErrorTag::Control, 0, Some(*cause))
                    }
                    SqlConstantEvaluationError::Constant(_) => (ErrorTag::Constant, 0, None),
                    SqlConstantEvaluationError::InvalidType(_) => (ErrorTag::InvalidType, 0, None),
                    SqlConstantEvaluationError::Preparation(_) => (ErrorTag::Preparation, 0, None),
                };
                Outcome::Error {
                    tag,
                    raw_text_len,
                    control,
                }
            }
        };
        // No capacity request, error-string cloning, evaluator, or name binding.
        let mut receipts = self.receipts.lock().unwrap();
        let receipt = receipts.last_mut().unwrap();
        assert!(receipt.outcome.is_none());
        receipt.outcome = Some(outcome);
    }
}
// Deliberately no observer methods and no query-specific mutable state.
struct ExistingEvaluator {
    mode: Mode,
}
impl SqlConstantEvaluator for ExistingEvaluator {
    fn eval_scalar(
        &self,
        request: &FoldRequest,
        control: &dyn PureCompileControl,
    ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
        calculate(&self.mode, request, control)
    }
}
static REAL: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::LegacyKernel,
};
static DECLINE: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Decline,
};
static CANCEL: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Control(
        CompileControlError::Cancelled,
    )),
};
static DEADLINE: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Control(
        CompileControlError::DeadlineExceeded,
    )),
};
static RESOURCE: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Control(
        CompileControlError::ResourceExhausted,
    )),
};
static LONG_ERROR: LazyLock<ExistingEvaluator> = LazyLock::new(|| ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Evaluation(
        "original folding evaluation failure ".repeat(40),
    )),
});
static CONSTANT_ERROR: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Constant(
        ConstantError::Invalid("original constant refusal"),
    )),
};
static TYPE_ERROR: ExistingEvaluator = ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::InvalidType(
        novarocks_type_contract::ValueTypeError::TooDeep,
    )),
};
static PREPARATION_ERROR: LazyLock<ExistingEvaluator> = LazyLock::new(|| ExistingEvaluator {
    mode: Mode::Fail(SqlConstantEvaluationError::Preparation(
        novarocks_functions::KernelFailure::InvalidProgram(
            novarocks_functions::KernelDiagnostic::new("original preparation refusal"),
        ),
    )),
});
struct MustNotEvaluate;
impl SqlConstantEvaluator for MustNotEvaluate {
    fn eval_scalar(
        &self,
        _: &FoldRequest,
        _: &dyn PureCompileControl,
    ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
        panic!("a refused observation must not perform original evaluation")
    }
}
static MUST_NOT_EVALUATE: MustNotEvaluate = MustNotEvaluate;
fn policy() -> novarocks_functions::ConstantPolicy {
    novarocks_functions::ConstantPolicy {
        max_rows: 64,
        max_array_nodes: 1024,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
fn compile_typed(
    sql: &str,
    evaluator: &'static dyn SqlConstantEvaluator,
    observer: Option<Arc<Probe>>,
) -> Result<SqlAuthoredPhysicalPlan, novarocks_sql::compiler::SqlCompileProgressError> {
    let control = match observer {
        Some(observer) => SqlCompileControl::unbounded().with_fold_dependency_observer(observer),
        None => SqlCompileControl::unbounded(),
    };
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([83; 16]).unwrap(),
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                ..SessionOptimizerSettings::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        evaluator,
        policy(),
        SqlPhysicalEmissionMode::OriginalNativeV1,
        control.clone(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: 64,
            max_batch_bytes: 1 << 20,
        },
        DEFAULT_COMPLETION_LIMITS,
    );
    match SqlCompiler::start(request.try_into_completion()?, &control)? {
        SqlCompileProgress::Complete(completed) => Ok(completed.into_plan()),
        SqlCompileProgress::Incomplete(need) => Err(
            novarocks_sql::compiler::SqlCompileError::InvalidRequest(format!(
                "real table-free fixture unexpectedly asked for facts: {need:?}"
            ))
            .into(),
        ),
    }
}
fn compile(
    sql: &str,
    evaluator: &'static dyn SqlConstantEvaluator,
    observer: Option<Arc<Probe>>,
) -> Result<SqlAuthoredPhysicalPlan, String> {
    compile_typed(sql, evaluator, observer).map_err(|error| error.to_string())
}
fn probe() -> Arc<Probe> {
    Arc::new(Probe::new())
}
fn calls(plan: &SqlAuthoredPhysicalPlan) -> usize {
    plan.plan()
        .fragments()
        .values()
        .flat_map(|f| f.expressions().iter())
        .filter(|(_, e)| matches!(e.kind, ExprKind::FunctionCall { .. }))
        .count()
}
fn text(value: &ConstantValue) -> String {
    let array = value.pool().array();
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(value.ordinal() as usize)
        .into()
}
#[test]
fn sql_fold_dependency_real_upper_removed_from_plan_keeps_exact_binding_and_cv() {
    let evaluator = probe();
    let source = compile(
        "SELECT upper('Straße') AS folded",
        &REAL,
        Some(evaluator.clone()),
    )
    .unwrap();
    assert_eq!(
        calls(&source),
        0,
        "successful original fold must remove its call"
    );
    let receipts = evaluator.receipts.lock().unwrap();
    let receipt = receipts
        .iter()
        .find(|r| r.binding.is_some())
        .expect("folded function dependency was lost");
    assert_eq!(text(&receipt.arguments[0].value), "Straße");
    match receipt.outcome.as_ref().unwrap() {
        Outcome::Produced(value) => {
            assert_eq!(text(value), "STRASSE");
            assert_eq!(value.value_type(), &receipt.request_result);
        }
        other => panic!("unexpected actual legacy result: {other:?}"),
    }
    assert!(receipts.iter().all(|receipt| receipt.outcome.is_some()));
}
#[test]
fn sql_fold_dependency_two_actual_functions_both_recorded_before_replacement() {
    let evaluator = probe();
    let source = compile(
        "SELECT upper('first') AS a, upper('second') AS b",
        &REAL,
        Some(evaluator.clone()),
    )
    .unwrap();
    assert_eq!(calls(&source), 0);
    let receipts = evaluator.receipts.lock().unwrap();
    let values = receipts
        .iter()
        .filter(|r| r.binding.is_some())
        .map(|r| text(&r.arguments[0].value))
        .collect::<Vec<_>>();
    assert!(values.iter().any(|v| v == "first"));
    assert!(values.iter().any(|v| v == "second"));
}
#[test]
fn sql_fold_dependency_cast_intrinsic_retains_exact_source_result_policy() {
    let evaluator = probe();
    let source = compile(
        "SELECT CAST(12 AS VARCHAR) AS folded_cast",
        &REAL,
        Some(evaluator.clone()),
    )
    .unwrap();
    assert_eq!(calls(&source), 0);
    let receipts = evaluator.receipts.lock().unwrap();
    let receipt = receipts
        .iter()
        .find(|r| matches!(r.request_kind, FoldNodeKind::Cast(_)))
        .expect("cast invocation was omitted");
    assert!(receipt.binding.is_none());
    assert_eq!(receipt.arguments[0].value.try_i64().unwrap(), Some(12));
    assert_eq!(
        receipt.request_result.data_type,
        arrow::datatypes::DataType::Utf8
    );
    assert!(matches!(receipt.outcome, Some(Outcome::Produced(_))));
}
#[test]
fn sql_fold_dependency_decline_and_long_evaluation_error_preserve_fail_open() {
    let long_len = "original folding evaluation failure ".repeat(40).len();
    for calculator in [&DECLINE as &'static dyn SqlConstantEvaluator, &*LONG_ERROR] {
        let observer = probe();
        let actual = compile(
            "SELECT upper('kept') AS kept",
            calculator,
            Some(observer.clone()),
        )
        .unwrap();
        let unchanged = compile("SELECT upper('kept') AS kept", calculator, None).unwrap();
        assert_eq!(calls(&actual), calls(&unchanged));
        assert!(calls(&actual) >= 1);
        let receipts = observer.receipts.lock().unwrap();
        for receipt in receipts.iter().filter(|r| r.binding.is_some()) {
            match receipt.outcome.as_ref().unwrap() {
                Outcome::Declined => {}
                Outcome::Error {
                    tag: ErrorTag::Evaluation,
                    raw_text_len,
                    ..
                } => {
                    assert_eq!(*raw_text_len, long_len);
                    assert!(*raw_text_len > 512);
                }
                other => panic!("unexpected result: {other:?}"),
            }
        }
    }
}
#[test]
fn sql_fold_dependency_start_control_refusal_has_no_evaluation_or_completion() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let observer = Arc::new(Probe {
            refuse_start: Some(cause),
            ..Probe::new()
        });
        let error = compile_typed(
            "SELECT upper('never')",
            &MUST_NOT_EVALUATE,
            Some(observer.clone()),
        )
        .unwrap_err();
        assert!(matches!(
            (cause, error),
            (
                CompileControlError::Cancelled,
                novarocks_sql::compiler::SqlCompileProgressError::Compile(
                    novarocks_sql::compiler::SqlCompileError::Cancelled
                )
            ) | (
                CompileControlError::DeadlineExceeded,
                novarocks_sql::compiler::SqlCompileProgressError::Compile(
                    novarocks_sql::compiler::SqlCompileError::DeadlineExceeded
                )
            ) | (
                CompileControlError::ResourceExhausted,
                novarocks_sql::compiler::SqlCompileProgressError::Compile(
                    novarocks_sql::compiler::SqlCompileError::ResourceExhausted
                )
            )
        ));
        assert!(observer.receipts.lock().unwrap().is_empty());
    }
}
#[test]
fn sql_fold_dependency_original_control_errors_stay_first_and_typed() {
    for (cause, calculator) in [
        (CompileControlError::Cancelled, &CANCEL),
        (CompileControlError::DeadlineExceeded, &DEADLINE),
        (CompileControlError::ResourceExhausted, &RESOURCE),
    ] {
        let observer = probe();
        let actual = compile(
            "SELECT upper('control')",
            calculator,
            Some(observer.clone()),
        )
        .unwrap_err();
        let original = compile("SELECT upper('control')", calculator, None).unwrap_err();
        assert_eq!(actual, original);
        let receipts = observer.receipts.lock().unwrap();
        assert_eq!(receipts.len(), 1);
        assert!(
            matches!(&receipts[0].outcome, Some(Outcome::Error { tag: ErrorTag::Control, control: Some(c), .. }) if *c == cause)
        );
    }
}
#[test]
fn sql_fold_dependency_original_static_failure_classes_and_text_unchanged() {
    for calculator in [
        &CONSTANT_ERROR as &'static dyn SqlConstantEvaluator,
        &TYPE_ERROR,
        &*PREPARATION_ERROR,
    ] {
        let observer = probe();
        let actual =
            compile("SELECT upper('static')", calculator, Some(observer.clone())).unwrap_err();
        let original = compile("SELECT upper('static')", calculator, None).unwrap_err();
        assert_eq!(actual, original);
        let receipts = observer.receipts.lock().unwrap();
        assert_eq!(receipts.len(), 1);
        assert!(matches!(
            &receipts[0].outcome,
            Some(Outcome::Error {
                tag: ErrorTag::Constant | ErrorTag::InvalidType | ErrorTag::Preparation,
                ..
            })
        ));
    }
}
#[test]
fn sql_fold_dependency_query_owned_receipts_are_isolated_and_released() {
    let left = probe();
    let right = probe();
    let left_weak = Arc::downgrade(&left);
    let right_weak = Arc::downgrade(&right);
    std::thread::scope(|scope| {
        scope.spawn(|| compile("SELECT upper('left')", &REAL, Some(left.clone())).unwrap());
        scope.spawn(|| compile("SELECT upper('right')", &REAL, Some(right.clone())).unwrap());
    });
    for (observer, expected) in [(&left, "left"), (&right, "right")] {
        let receipts = observer.receipts.lock().unwrap();
        let values = receipts
            .iter()
            .filter(|r| r.binding.is_some())
            .map(|r| text(&r.arguments[0].value))
            .collect::<Vec<_>>();
        assert!(!values.is_empty());
        assert!(values.iter().all(|value| value == expected));
    }
    drop(left);
    drop(right);
    assert!(left_weak.upgrade().is_none());
    assert!(right_weak.upgrade().is_none());
}

#[test]
fn sql_fold_dependency_observer_absence_keeps_existing_calculator_path() {
    let source = compile("SELECT upper('plain')", &REAL, None).unwrap();
    assert_eq!(calls(&source), 0);
}

#[test]
fn sql_fold_dependency_original_unconstrained_binding_is_not_selected_result_constraint() {
    let observer = probe();
    let source = compile(
        "SELECT upper('unconstrained') AS folded",
        &REAL,
        Some(observer.clone()),
    )
    .unwrap();
    assert_eq!(calls(&source), 0);
    let receipts = observer.receipts.lock().unwrap();
    let receipt = receipts
        .iter()
        .find(|receipt| receipt.binding.is_some())
        .expect("actual original function fold must be observed");
    let binding = receipt.binding.as_ref().unwrap();
    assert_eq!(
        binding.selected.result_type,
        FunctionResultType::Scalar(receipt.request_result.clone())
    );
    assert!(receipt.source_result_constraint.is_none());
    assert_eq!(
        receipt.source_constraint_origin,
        Some(novarocks_sql::binding::SqlResultConstraintOrigin::Unconstrained)
    );
    assert_eq!(
        receipt.source_decimal_policy,
        Some(
            novarocks_sql::sql_mode::SqlSemanticSettings::default()
                .sql_mode()
                .decimal_overflow_policy()
        )
    );
    assert!(matches!(receipt.outcome, Some(Outcome::Produced(_))));
}
