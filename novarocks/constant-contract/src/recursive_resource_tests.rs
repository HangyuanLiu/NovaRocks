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
use arrow_array::{Int64Array, ListArray, StringArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::UnionFields;
use std::sync::Mutex;

const PHASE: CompilePhase = CompilePhase::Decode;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    failure: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            failure: None,
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            failure: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((refusal, _)) = self.failure {
            assert!(at <= refusal, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.failure {
            Some((refusal, cause)) if at == refusal => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1_000_000,
        max_array_nodes: 4096,
        max_logical_elements: 100_000_000,
        max_retained_buffer_bytes: 64 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1024 * 1024,
        max_library_validation_work: 100_000_000,
        max_library_validation_bytes: 100_000_000,
    }
}
fn input() -> RecursiveConstantResourceInput {
    RecursiveConstantResourceInput {
        buffer_count_upper_bound: 17,
        buffer_visits_bytes_upper_bound: 100,
        retained_buffer_capacity_bytes_upper_bound: 512,
        view_validation_bytes_upper_bound: 20,
        utf8_fallback_validation_bytes_upper_bound: 10,
    }
}
fn source_field(ty: DataType) -> Field {
    Field::new("source", ty, true)
}
fn ty(field: &Field) -> FunctionValueType {
    FunctionValueType::new(field.data_type().clone(), field.is_nullable())
}
fn project(
    field: &Field,
    lengths: &[u64],
    input: RecursiveConstantResourceInput,
    policy: ConstantPolicy,
    control: &Control,
) -> Result<ConstantResourceFacts, ConstantError> {
    preflight_recursive_pool_resources(
        field,
        &ty(field),
        lengths.iter().copied(),
        input,
        policy,
        PHASE,
        control,
    )
}
fn check_prefixes(
    field: &Field,
    lengths: &[u64],
    input: RecursiveConstantResourceInput,
    policy: ConstantPolicy,
) {
    let control = Control::good();
    let _ = project(field, lengths, input, policy, &control);
    let trace = control.trace();
    assert!(
        trace.len() >= 2,
        "ordinary error or success omitted its tail"
    );
    assert!(trace.iter().all(|(_, units)| *units <= 256));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::refusing(at, cause);
            assert!(
                matches!(project(field,lengths,input,policy,&control), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
fn shape() -> Field {
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Int64, false)),
                Arc::new(Field::new("value", DataType::Utf8, true)),
            ]
            .into(),
        ),
        false,
    );
    source_field(DataType::Struct(
        vec![
            Arc::new(Field::new(
                "list",
                DataType::List(Arc::new(Field::new("item", DataType::Int8, true))),
                true,
            )),
            Arc::new(Field::new(
                "map",
                DataType::Map(Arc::new(entries), false),
                true,
            )),
            Arc::new(Field::new(
                "large",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Null, true))),
                true,
            )),
        ]
        .into(),
    ))
}
const LENGTHS: [u64; 9] = [2, 2, 5, 2, 3, 3, 3, 2, 4];

#[test]
fn recursive_projection_uses_complete_child_extents_and_the_original_numerical_formula() {
    let field = shape();
    let facts = project(&field, &LENGTHS, input(), policy(), &Control::good()).unwrap();
    // 26 stored rows; root max-value = 1 + (1+5) + (1+3*(1+1+1)) + (1+4).
    assert_eq!(facts.rows, 2);
    assert_eq!(facts.array_nodes, 9);
    assert_eq!(facts.logical_elements_upper_bound, 44);
    assert_eq!(facts.buffer_count, 17);
    assert_eq!(facts.retained_buffer_capacity_bytes, 512);
    let metadata = facts.metadata_bytes;
    assert_eq!(
        facts.library_validation_work_upper_bound,
        1016 + 4 * metadata
    );
    let headers = 9 * std::mem::size_of::<ArrayData>() as u64
        + 17 * std::mem::size_of::<arrow_buffer::Buffer>() as u64;
    let diagnostics = (metadata + 9 * 256) * 4 * 16;
    assert_eq!(
        facts.library_validation_temporary_bytes_upper_bound,
        headers + diagnostics
    );
    assert_eq!(
        facts.library_validation_bytes_upper_bound,
        130 + headers + diagnostics
    );
    // An empty root cannot erase the complete child extent or its policy cost.
    let mut empty = LENGTHS;
    empty[0] = 0;
    let facts = project(&field, &empty, input(), policy(), &Control::good()).unwrap();
    assert_eq!(facts.logical_elements_upper_bound, 24);
    check_prefixes(&field, &LENGTHS, input(), policy());
}

fn raw_facts(data: &ArrayData, lengths: &mut Vec<u64>, input: &mut RecursiveConstantResourceInput) {
    lengths.push(data.len() as u64);
    for b in data
        .buffers()
        .iter()
        .chain(data.nulls().map(|n| n.buffer()))
    {
        input.buffer_count_upper_bound += 1;
        input.buffer_visits_bytes_upper_bound += b.len() as u64;
        input.retained_buffer_capacity_bytes_upper_bound += b.capacity().max(b.len()) as u64;
    }
    if matches!(data.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        input.utf8_fallback_validation_bytes_upper_bound += data.buffers()[1].len() as u64;
    }
    for child in data.child_data() {
        raw_facts(child, lengths, input);
    }
}
#[test]
fn real_sliced_nested_pool_is_dominated_and_keeps_hidden_null_child_payload() {
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 6])),
        Arc::new(Int64Array::from(vec![9, 10, 11, 12, 13, 14])),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let fields = vec![
        Arc::new(Field::new("list", list.data_type().clone(), true)),
        Arc::new(Field::new("text", DataType::Utf8, true)),
    ]
    .into();
    let root = StructArray::new(
        fields,
        vec![
            Arc::new(list),
            Arc::new(StringArray::from(vec![Some("prefix"), None, Some("last")])),
        ],
        None,
    )
    .slice(1, 2);
    let data = root.to_data();
    let field = Arc::new(source_field(root.data_type().clone()));
    let value_type = ty(&field);
    let mut lengths = Vec::new();
    let mut input = RecursiveConstantResourceInput {
        buffer_count_upper_bound: 0,
        buffer_visits_bytes_upper_bound: 0,
        retained_buffer_capacity_bytes_upper_bound: 0,
        view_validation_bytes_upper_bound: 0,
        utf8_fallback_validation_bytes_upper_bound: 0,
    };
    raw_facts(&data, &mut lengths, &mut input);
    assert_eq!(lengths, [2, 2, 6, 2]);
    let bound = project(&field, &lengths, input, policy(), &Control::good()).unwrap();
    // Complete List values has six rows, including sliced-away prefix and
    // the two hidden rows below the selected NULL list.
    assert_eq!(bound.logical_elements_upper_bound, 18);
    let pool =
        ConstantPool::try_new(field, value_type, data, policy(), PHASE, &Control::good()).unwrap();
    let actual = pool.resource_facts();
    assert_eq!(actual.rows, bound.rows);
    assert_eq!(actual.array_nodes, bound.array_nodes);
    assert_eq!(
        actual.logical_elements_upper_bound,
        bound.logical_elements_upper_bound
    );
    assert_eq!(actual.buffer_count, bound.buffer_count);
    assert_eq!(actual.metadata_bytes, bound.metadata_bytes);
    assert_eq!(
        actual.library_validation_work_upper_bound,
        bound.library_validation_work_upper_bound
    );
    assert_eq!(
        actual.library_validation_bytes_upper_bound,
        bound.library_validation_bytes_upper_bound
    );
    assert!(actual.retained_buffer_capacity_bytes <= bound.retained_buffer_capacity_bytes);
}

#[test]
fn recursive_projection_checks_exact_source_shape_and_never_skips_unsupported_empty_children() {
    let source = shape();
    for lengths in [&LENGTHS[..8], &[2, 2, 5, 2, 3, 3, 3, 2, 4, 0][..]] {
        assert!(matches!(
            project(&source, lengths, input(), policy(), &Control::good()),
            Err(ConstantError::Invalid(_))
        ));
        check_prefixes(&source, lengths, input(), policy());
    }
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let unsupported = [
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int64)),
        DataType::FixedSizeList(item.clone(), 1),
        DataType::ListView(item.clone()),
        DataType::LargeListView(item.clone()),
        DataType::Union(
            UnionFields::try_new(vec![0], vec![item.clone()]).unwrap(),
            UnionMode::Sparse,
        ),
        DataType::RunEndEncoded(Arc::new(Field::new("ends", DataType::Int16, false)), item),
    ];
    for carrier in unsupported {
        let source = source_field(DataType::Struct(
            vec![Arc::new(Field::new("child", carrier, true))].into(),
        ));
        assert_eq!(
            project(&source, &[0, 0], input(), policy(), &Control::good()).unwrap_err(),
            ConstantError::Invalid(
                "constant resource projection has an unsupported recursive carrier"
            )
        );
        check_prefixes(&source, &[0, 0], input(), policy());
    }
    let source = source_field(DataType::Int64);
    let wrong = FunctionValueType::new(DataType::Int32, true);
    assert!(
        preflight_recursive_pool_resources(
            &source,
            &wrong,
            [1],
            input(),
            policy(),
            PHASE,
            &Control::good()
        )
        .is_err()
    );
    let mut wrong_nullable = ty(&source);
    wrong_nullable.nullable = false;
    assert!(
        preflight_recursive_pool_resources(
            &source,
            &wrong_nullable,
            [1],
            input(),
            policy(),
            PHASE,
            &Control::good()
        )
        .is_err()
    );
}

#[test]
fn recursive_projection_limits_and_arithmetic_overflow_refuse_before_materialization() {
    let field = shape();
    let facts = project(&field, &LENGTHS, input(), policy(), &Control::good()).unwrap();
    let mut exact = policy();
    exact.max_rows = 2;
    exact.max_array_nodes = 9;
    exact.max_logical_elements = 44;
    exact.max_retained_buffer_bytes = 512;
    exact.max_type_depth = 4;
    exact.max_metadata_bytes = facts.metadata_bytes;
    exact.max_library_validation_work = facts.library_validation_work_upper_bound;
    exact.max_library_validation_bytes = facts.library_validation_bytes_upper_bound;
    assert_eq!(
        project(&field, &LENGTHS, input(), exact, &Control::good()).unwrap(),
        facts
    );
    for dimension in 0..7 {
        let mut over = exact;
        match dimension {
            0 => over.max_rows -= 1,
            1 => over.max_array_nodes -= 1,
            2 => over.max_logical_elements -= 1,
            3 => over.max_retained_buffer_bytes -= 1,
            4 => over.max_type_depth -= 1,
            5 => over.max_library_validation_work -= 1,
            _ => over.max_library_validation_bytes -= 1,
        }
        assert!(matches!(
            project(&field, &LENGTHS, input(), over, &Control::good()),
            Err(ConstantError::Limit(_))
        ));
        check_prefixes(&field, &LENGTHS, input(), over);
    }
    let mut permissive = policy();
    permissive.max_rows = u64::MAX;
    permissive.max_logical_elements = u64::MAX;
    permissive.max_library_validation_work = u64::MAX;
    permissive.max_library_validation_bytes = u64::MAX;
    let list = source_field(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Int64,
        true,
    ))));
    assert!(matches!(
        project(
            &list,
            &[u64::MAX / 2 + 1, 1],
            input(),
            permissive,
            &Control::good()
        ),
        Err(ConstantError::Limit(
            "constant resource arithmetic overflow"
        ))
    ));
    let mut overflow = input();
    overflow.buffer_visits_bytes_upper_bound = u64::MAX;
    assert!(matches!(
        project(&field, &LENGTHS, overflow, permissive, &Control::good()),
        Err(ConstantError::Limit(
            "constant resource arithmetic overflow"
        ))
    ));
    check_prefixes(&field, &LENGTHS, overflow, permissive);
}

#[test]
fn flat_and_recursive_resource_projections_share_the_same_validation_envelope() {
    for carrier in [
        DataType::Null,
        DataType::Int64,
        DataType::Utf8,
        DataType::Utf8View,
    ] {
        let field = source_field(carrier);
        let input = input();
        let flat = preflight_flat_pool_resources(
            &field,
            &ty(&field),
            FlatConstantResourceInput {
                rows: 13,
                buffer_count_upper_bound: input.buffer_count_upper_bound,
                // The old flat API includes fallback in total visits.
                buffer_visits_bytes_upper_bound: input.buffer_visits_bytes_upper_bound
                    + input.utf8_fallback_validation_bytes_upper_bound,
                retained_buffer_capacity_bytes_upper_bound: input
                    .retained_buffer_capacity_bytes_upper_bound,
                view_validation_bytes_upper_bound: input.view_validation_bytes_upper_bound,
            },
            policy(),
            PHASE,
            &Control::good(),
        )
        .unwrap();
        let recursive = project(&field, &[13], input, policy(), &Control::good()).unwrap();
        assert_eq!(recursive.metadata_bytes, flat.metadata_bytes);
        assert_eq!(
            recursive.library_validation_work_upper_bound,
            flat.library_validation_work_upper_bound
        );
        assert_eq!(
            recursive.library_validation_bytes_upper_bound,
            flat.library_validation_bytes_upper_bound
        );
        assert_eq!(
            recursive.library_validation_temporary_bytes_upper_bound,
            flat.library_validation_temporary_bytes_upper_bound
        );
    }
    let empty_struct = source_field(DataType::Struct(Vec::<Arc<Field>>::new().into()));
    assert_eq!(
        project(&empty_struct, &[7], input(), policy(), &Control::good())
            .unwrap()
            .logical_elements_upper_bound,
        7
    );
}

#[test]
fn recursive_projection_quantum_and_ordinary_tail_preserve_all_original_causes() {
    let field = source_field(DataType::Struct(
        (0..320)
            .map(|i| Arc::new(Field::new(format!("f{i}"), DataType::Int64, true)))
            .collect::<Vec<_>>()
            .into(),
    ));
    let lengths = vec![2; 321];
    let control = Control::good();
    project(&field, &lengths, input(), policy(), &control).unwrap();
    let trace = control.trace();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    assert!(trace.last().unwrap().1 > 0);
    check_prefixes(&field, &lengths, input(), policy());
    let mut extra = lengths;
    extra.push(0);
    check_prefixes(&field, &extra, input(), policy());
}
