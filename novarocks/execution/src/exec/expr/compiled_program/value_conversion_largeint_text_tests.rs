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
//! Actual frozen hidden conversion: exact nominal source, typed output and old oracle.
use super::*;
#[test]
fn nominal_largeint_text_actual_compiler_matches_v1_sparse_min_max_null_slice_empty() {
    let target = FunctionValueType::new(DataType::Utf8, true);
    let program = direct_program(largeint_type(), target.clone());
    assert_eq!(result_type(&program), target);
    let raw = wide(&[Some(0), Some(i128::MIN), Some(42), None, Some(i128::MAX)]).slice(1, 4);
    let batch = input_batch(&program, vec![raw.clone()]);
    let rows = [0, 1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let mut evaluator = instance(&program);
    for _ in 0..2 {
        let output = evaluator.evaluate(&batch, selection, &Control).unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output.values().to_data(),
            crate::exec::expr::cast::cast_with_special_rules(&raw, &DataType::Utf8)
                .unwrap()
                .to_data()
        );
    }
    let output = evaluator
        .evaluate(&batch, Selection::try_sparse(4, &[]).unwrap(), &Control)
        .unwrap();
    assert_eq!(
        output.values().to_data(),
        StringArray::from(Vec::<Option<&str>>::new()).to_data()
    );
}
