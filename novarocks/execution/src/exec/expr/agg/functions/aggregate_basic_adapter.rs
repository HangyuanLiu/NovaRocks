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
//! Legacy state storage projection for the shared pure basic aggregate core.
use super::super::*;
use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::DataType;
use novarocks_functions::builtin::aggregate_basic::{
    BasicComputation, BasicOperation, BasicState, BasicStateDomain, BasicStateError, BasicValue,
};
use novarocks_functions::{EvaluationCheckpoints, KernelEvaluationControl, KernelFailure};

struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("basic aggregate requested a wait"),
        ))
    }
}
pub(super) fn legacy_error(e: impl Into<BasicStateError>) -> String {
    e.into().into_legacy_message()
}
fn operation(kind: &AggKind) -> BasicOperation {
    use BasicOperation::*;
    match kind {
        AggKind::AvgInt | AggKind::AvgFloat | AggKind::AvgDecimal128 | AggKind::AvgDecimal256 => {
            Avg
        }
        AggKind::CountIf => CountIf,
        AggKind::BoolOr => Or,
        AggKind::BoolAnd => And,
        AggKind::VariancePop => VarPop,
        AggKind::VarianceSamp => VarSamp,
        AggKind::StddevPop => StdPop,
        AggKind::StddevSamp => StdSamp,
        AggKind::CovarPop => CovPop,
        AggKind::CovarSamp => CovSamp,
        AggKind::Corr => Corr,
        _ => unreachable!("foreign basic aggregate kind"),
    }
}
fn core(spec: &AggSpec) -> BasicComputation<'_> {
    BasicComputation {
        operation: operation(&spec.kind),
        domain: match spec.kind {
            AggKind::AvgDecimal128 => BasicStateDomain::Decimal128,
            AggKind::AvgDecimal256 => BasicStateDomain::Decimal256,
            AggKind::AvgInt
            | AggKind::AvgFloat
            | AggKind::CountIf
            | AggKind::BoolOr
            | AggKind::BoolAnd
            | AggKind::VariancePop
            | AggKind::VarianceSamp
            | AggKind::StddevPop
            | AggKind::StddevSamp
            | AggKind::CovarPop
            | AggKind::CovarSamp
            | AggKind::Corr => BasicStateDomain::Plain,
            _ => unreachable!("foreign basic state domain"),
        },
        input_scale: match (&spec.kind, spec.input_arg_type.as_ref()) {
            (AggKind::AvgDecimal128, Some(DataType::Decimal128(_, s)))
            | (AggKind::AvgDecimal256, Some(DataType::Decimal256(_, s))) => Some(*s),
            _ => None,
        },
        output_type: &spec.output_type,
        intermediate_type: &spec.intermediate_type,
    }
}
fn load(spec: &AggSpec, p: *const u8) -> BasicState {
    let mut out = BasicState::default();
    unsafe {
        match spec.kind {
            AggKind::CountIf => out.count = *(p as *const i64),
            AggKind::BoolOr | AggKind::BoolAnd => {
                let s = &*(p as *const BoolState);
                out.seen = s.has_value;
                out.boolean = s.value;
            }
            AggKind::AvgInt | AggKind::AvgFloat => {
                let s = &*(p as *const AvgState);
                out.sum = s.sum;
                out.count = s.count;
            }
            AggKind::AvgDecimal128 => {
                let s = &*(p as *const AvgDecimal128State);
                out.decimal = s.sum;
                out.count = s.count;
            }
            AggKind::AvgDecimal256 => {
                let s = &*(p as *const AvgDecimal256State);
                out.wide = s.sum;
                out.count = s.count;
            }
            AggKind::VariancePop
            | AggKind::VarianceSamp
            | AggKind::StddevPop
            | AggKind::StddevSamp => {
                let s = &*(p as *const DevFromAveState);
                out.x = s.mean;
                out.m2 = s.m2;
                out.count = s.count;
            }
            AggKind::CovarPop | AggKind::CovarSamp => {
                let s = &*(p as *const CovarState);
                out.x = s.mean_x;
                out.y = s.mean_y;
                out.m2 = s.c2;
                out.count = s.count;
            }
            AggKind::Corr => {
                let s = &*(p as *const CorrState);
                out.x = s.mean_x;
                out.y = s.mean_y;
                out.m2 = s.c2;
                out.m2x = s.m2x;
                out.m2y = s.m2y;
                out.count = s.count;
            }
            _ => unreachable!("foreign basic aggregate state"),
        }
    }
    out
}
fn store(spec: &AggSpec, p: *mut u8, s: BasicState) {
    unsafe {
        match spec.kind {
            AggKind::CountIf => *(p as *mut i64) = s.count,
            AggKind::BoolOr | AggKind::BoolAnd => {
                let out = &mut *(p as *mut BoolState);
                out.has_value = s.seen;
                out.value = s.boolean;
            }
            AggKind::AvgInt | AggKind::AvgFloat => {
                let out = &mut *(p as *mut AvgState);
                out.sum = s.sum;
                out.count = s.count;
            }
            AggKind::AvgDecimal128 => {
                let out = &mut *(p as *mut AvgDecimal128State);
                out.sum = s.decimal;
                out.count = s.count;
            }
            AggKind::AvgDecimal256 => {
                let out = &mut *(p as *mut AvgDecimal256State);
                out.sum = s.wide;
                out.count = s.count;
            }
            AggKind::VariancePop
            | AggKind::VarianceSamp
            | AggKind::StddevPop
            | AggKind::StddevSamp => {
                let out = &mut *(p as *mut DevFromAveState);
                out.mean = s.x;
                out.m2 = s.m2;
                out.count = s.count;
            }
            AggKind::CovarPop | AggKind::CovarSamp => {
                let out = &mut *(p as *mut CovarState);
                out.mean_x = s.x;
                out.mean_y = s.y;
                out.c2 = s.m2;
                out.count = s.count;
            }
            AggKind::Corr => {
                let out = &mut *(p as *mut CorrState);
                out.mean_x = s.x;
                out.mean_y = s.y;
                out.c2 = s.m2;
                out.m2x = s.m2x;
                out.m2y = s.m2y;
                out.count = s.count;
            }
            _ => unreachable!("foreign basic aggregate state"),
        }
    }
}
fn raw_value(
    input: &AggInputView,
    row: usize,
    w: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<BasicValue>, KernelFailure> {
    Ok(match input {
        AggInputView::Bool(a) => (!a.is_null(row)).then(|| BasicValue::Boolean(a.value(row))),
        AggInputView::Int(v) => v.value_at(row).map(|v| BasicValue::Float(v as f64)),
        AggInputView::Float(v) => v.value_at(row).map(BasicValue::Float),
        AggInputView::Utf8(Utf8ArrayView::Decimal128(a, s)) => {
            (!a.is_null(row)).then(|| BasicValue::Decimal128(a.value(row), *s))
        }
        AggInputView::Any(a) => {
            novarocks_functions::builtin::aggregate_basic::read_basic_value(a.as_ref(), row, w)?
        }
        _ => unreachable!("validated basic aggregate raw carrier"),
    })
}
pub(super) fn update(
    spec: &AggSpec,
    offset: usize,
    states: &[AggStatePtr],
    input: &AggInputView,
    state_scale: Option<i8>,
) -> Result<(), String> {
    let core = core(spec);
    let mut w = EvaluationCheckpoints::new(&LegacyControl);
    let pair = if matches!(
        spec.kind,
        AggKind::CovarPop | AggKind::CovarSamp | AggKind::Corr
    ) {
        let AggInputView::Any(array) = input else {
            return Err("covar/corr batch input type mismatch".to_owned());
        };
        let a = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "covar/corr expects struct input".to_owned())?;
        if a.num_columns() != 2 {
            return Err("covar/corr expects 2 arguments".to_owned());
        }
        for a in a.columns() {
            if !matches!(
                a.data_type(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
            ) {
                return Err(format!(
                    "covar/corr unsupported input type: {:?}",
                    a.data_type()
                ));
            }
        }
        Some(a)
    } else {
        None
    };
    for (row, &base) in states.iter().enumerate() {
        let p = unsafe { (base as *mut u8).add(offset) };
        let mut s = load(spec, p);
        let (first, second) = if let Some(a) = pair {
            (
                novarocks_functions::builtin::aggregate_basic::read_basic_value(
                    a.column(0).as_ref(),
                    row,
                    &mut w,
                )
                .map_err(legacy_error)?,
                novarocks_functions::builtin::aggregate_basic::read_basic_value(
                    a.column(1).as_ref(),
                    row,
                    &mut w,
                )
                .map_err(legacy_error)?,
            )
        } else {
            (raw_value(input, row, &mut w).map_err(legacy_error)?, None)
        };
        core.update(&mut s, first, second, state_scale, &mut w)
            .map_err(legacy_error)?;
        store(spec, p, s);
    }
    w.finish_result(Ok(())).map_err(legacy_error)
}
pub(super) fn merge(
    spec: &AggSpec,
    offset: usize,
    states: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let core = core(spec);
    let mut w = EvaluationCheckpoints::new(&LegacyControl);
    for (row, &base) in states.iter().enumerate() {
        let incoming = match input {
            AggInputView::Int(v) => v.value_at(row).map(|count| BasicState {
                count,
                ..Default::default()
            }),
            AggInputView::AvgState(v) => v.value_at(row).map(|(sum, count)| BasicState {
                sum,
                count,
                ..Default::default()
            }),
            AggInputView::AvgDecimalState(v) => {
                v.value_at(row).map(|(decimal, count)| BasicState {
                    decimal,
                    count,
                    ..Default::default()
                })
            }
            AggInputView::Bool(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(core.decode_state(*a, row, &mut w).map_err(legacy_error)?)
                }
            }
            AggInputView::Binary(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(core.decode_state(*a, row, &mut w).map_err(legacy_error)?)
                }
            }
            AggInputView::Utf8(v) => {
                if let Some(text) = v.value_at(row) {
                    Some(core.decode_text(&text, &mut w).map_err(legacy_error)?)
                } else {
                    None
                }
            }
            AggInputView::Any(a) => core
                .decode_optional(a.as_ref(), row, &mut w)
                .map_err(legacy_error)?,
            _ => unreachable!("validated basic aggregate merge carrier"),
        };
        if let Some(incoming) = incoming {
            let p = unsafe { (base as *mut u8).add(offset) };
            let mut s = load(spec, p);
            core.merge_state(&mut s, incoming).map_err(legacy_error)?;
            store(spec, p, s);
        }
    }
    w.finish_result(Ok(())).map_err(legacy_error)
}
pub(super) fn build(
    spec: &AggSpec,
    offset: usize,
    states: &[AggStatePtr],
    intermediate: bool,
) -> Result<ArrayRef, String> {
    if !intermediate {
        match (&spec.kind, &spec.output_type) {
            (AggKind::AvgDecimal128, DataType::Decimal128(..))
            | (AggKind::AvgDecimal256, DataType::Decimal256(..)) => {}
            (AggKind::AvgDecimal128, t) => {
                return Err(format!("decimal output type mismatch: {:?}", t));
            }
            (AggKind::AvgDecimal256, t) => {
                return Err(format!("decimal256 output type mismatch: {:?}", t));
            }
            _ => {}
        }
    }
    if !intermediate
        && matches!(spec.intermediate_type, DataType::Binary | DataType::Utf8)
        && spec.input_arg_type.is_none()
    {
        match spec.kind {
            AggKind::AvgDecimal128 => return Err("avg decimal arg scale missing".to_owned()),
            AggKind::AvgDecimal256 => return Err("avg decimal256 arg scale missing".to_owned()),
            _ => {}
        }
    }
    core(spec)
        .build(
            states
                .iter()
                .map(|&base| Ok(load(spec, unsafe { (base as *const u8).add(offset) }))),
            intermediate,
            &LegacyControl,
        )
        .map_err(legacy_error)
}
