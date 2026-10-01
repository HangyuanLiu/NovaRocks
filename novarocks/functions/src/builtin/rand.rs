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

//! Instance-owned RNG computation for the exact RAND/RANDOM owners.
//! Arrow allocation requires host memory admission; representability is not a grant.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, builder::Float64Builder};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use rand::{
    Rng, SeedableRng,
    rngs::{OsRng, StdRng},
};

use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, ScalarCallInput, ScalarKernelInstance, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

/// Chosen by the exact private preparation owner, never by runtime array shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SeedRecipe {
    Unseeded,
    Constant(u64),
    PerRow,
}

pub(super) struct RandInstance {
    recipe: SeedRecipe,
    sequence: Option<StdRng>,
}
impl RandInstance {
    /// Construction does not acquire entropy or advance a sequence.
    pub(super) const fn new(recipe: SeedRecipe) -> Self {
        Self {
            recipe,
            sequence: None,
        }
    }
    pub(super) const fn retained_upper_bound() -> usize {
        std::mem::size_of::<Self>()
    }
    fn sequence(&mut self) -> Result<&mut StdRng, KernelFailure> {
        if self.sequence.is_none() {
            let rng = match self.recipe {
                SeedRecipe::Constant(seed) => StdRng::seed_from_u64(seed),
                SeedRecipe::Unseeded => StdRng::from_rng(OsRng).map_err(|_| {
                    KernelFailure::Operational(KernelDiagnostic::new(
                        "RAND entropy source could not initialize its instance",
                    ))
                })?,
                SeedRecipe::PerRow => {
                    return Err(internal("per-row RAND cannot retain a sequence"));
                }
            };
            self.sequence = Some(rng);
        }
        self.sequence
            .as_mut()
            .ok_or_else(|| internal("RAND sequence was not initialized"))
    }
}

impl ScalarKernelInstance for RandInstance {
    fn retained_bytes(&self) -> usize {
        // StdRng has inline state and no separately owned heap allocation.
        Self::retained_upper_bound()
    }
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(0)?;
        let result_type = input.contract().result_type();
        if result_type.logical_type != ValueLogicalType::Physical
            || result_type.data_type != DataType::Float64
        {
            return Err(invalid("RAND requires its exact Physical Float64 result"));
        }
        let seed = match self.recipe {
            SeedRecipe::Unseeded => {
                if !input.arguments().is_empty()
                    || !input.contract().selected().argument_types.is_empty()
                {
                    return Err(invalid("unseeded RAND requires no arguments"));
                }
                None
            }
            SeedRecipe::Constant(_) | SeedRecipe::PerRow => {
                let [FunctionArgumentType::Value(value_type)] =
                    input.contract().selected().argument_types.as_ref()
                else {
                    return Err(invalid("seeded RAND requires one exact value argument"));
                };
                if value_type.logical_type != ValueLogicalType::Physical
                    || value_type.data_type != DataType::Int64
                {
                    return Err(invalid("seeded RAND requires an exact Physical Int64 seed"));
                }
                let [argument] = input.arguments() else {
                    return Err(invalid("seeded RAND requires one evaluated seed"));
                };
                let array = argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| internal("RAND selected Int64 seed cannot be downcast"))?;
                Some((*argument, array, value_type.nullable))
            }
        };
        let selection = input.selection();
        output_capacity(selection.len())?;
        if let SeedRecipe::Constant(expected) = self.recipe {
            let (argument, array, nullable) =
                seed.ok_or_else(|| internal("RAND constant seed is absent"))?;
            // Validate only the selected invocation domain before any RNG
            // initialization or advancement. This is not a second resource budget.
            let mut work = EvaluationCheckpoints::new(control);
            for (ordinal, batch_row) in selection.iter().enumerate() {
                work.step()?;
                if selected_seed(argument, array, ordinal, batch_row, nullable)? != expected {
                    return Err(invalid(
                        "RAND evaluated seed differs from its frozen constant recipe",
                    ));
                }
            }
            work.finish()?;
        }
        let mut builder = Float64Builder::with_capacity(selection.len());
        let mut work = EvaluationCheckpoints::new(control);
        for (ordinal, batch_row) in selection.iter().enumerate() {
            work.step()?;
            let value = match self.recipe {
                SeedRecipe::Unseeded | SeedRecipe::Constant(_) => self.sequence()?.r#gen::<f64>(),
                SeedRecipe::PerRow => {
                    let (argument, array, nullable) =
                        seed.ok_or_else(|| internal("RAND per-row seed is absent"))?;
                    let actual = selected_seed(argument, array, ordinal, batch_row, nullable)?;
                    StdRng::seed_from_u64(actual).r#gen::<f64>()
                }
            };
            builder.append_value(value);
        }
        let values = Arc::new(builder.finish()) as ArrayRef;
        work.finish()?;
        SelectedValues::try_new(selection, &result_type.data_type, values, Box::default())
            .map_err(|_| internal("RAND compact output violates its selected contract"))
    }
}

fn selected_seed(
    argument: EvaluatedArgument<'_>,
    array: &Int64Array,
    ordinal: usize,
    batch_row: usize,
    nullable: bool,
) -> Result<u64, KernelFailure> {
    let row = argument.value_row(ordinal, batch_row);
    if row >= array.len() {
        return Err(internal("RAND seed row lies outside its checked carrier"));
    }
    if array.is_null(row) {
        if !nullable {
            return Err(internal("RAND non-null seed contains a selected SQL NULL"));
        }
        Ok(0)
    } else {
        Ok(array.value(row) as u64)
    }
}

/// Checked representability only, before builder allocation or RNG creation.
fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = rows
        .checked_mul(std::mem::size_of::<f64>())
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    isize::try_from(values).map_err(|_| KernelFailure::ResourceExhausted)?;
    isize::try_from(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConstantPolicy, ConstantPool, FunctionLiteral, FunctionValueType, ScalarEvaluationInstance,
        Selection,
    };
    use arrow_array::Float64Array;
    use novarocks_type_contract::CompilePhase;
    use std::{sync::Mutex, time::Duration};

    // Independently captured from the locked rand 0.8.5 StdRng reference,
    // not generated by the NovaRocks owner or this implementation under test.
    const ZERO: [u64; 6] = [
        0x3fe76547f659a58d,
        0x3fe8c02f9291c4ed,
        0x3f9a77040b3cc420,
        0x3fe2b16ec3b57cbe,
        0x3fd0c7675537b85e,
        0x3fe8b41de223ee38,
    ];
    const ONE: [u64; 6] = [
        0x3fef2d034c9a6603,
        0x3fe61e9a24b981ad,
        0x3fdb63f0568c9232,
        0x3fc6799a8b9bb210,
        0x3fd04a9378b14f5c,
        0x3feecda4583092a2,
    ];
    const NEGATIVE: [u64; 6] = [
        0x3faf4f30905c7ab0,
        0x3fa466e168822480,
        0x3fb2a43d6f65c610,
        0x3fd7420be4539448,
        0x3fc88a2269682a0c,
        0x3fe1e15670da7b72,
    ];
    const FORTY_TWO: [u64; 6] = [
        0x3fe0d98eec6444e4,
        0x3fe15e014267f5aa,
        0x3fe45dec0e3bca26,
        0x3fd9fa4b5e3f5d8c,
        0x3fa19561bff02330,
        0x3fda8ea728e783e0,
    ];

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl Control {
        fn refusing(index: usize, error: KernelFailure) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                refusal: Some((index, error)),
            }
        }
        fn calls(&self) -> Vec<u32> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let mut calls = self.calls.lock().unwrap();
            let index = calls.len();
            calls.push(units);
            if let Some((at, error)) = &self.refusal
                && *at == index
            {
                return Err(error.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("RAND must never wait");
        }
    }
    fn prepared(
        name: &str,
        source: Option<FunctionValueType>,
        literal: Option<FunctionLiteral>,
    ) -> Arc<dyn crate::PreparedScalarKernel> {
        super::super::rand_owner::prepared_for_test(name, source, literal).unwrap()
    }
    fn instance(name: &str, literal: Option<FunctionLiteral>) -> ScalarEvaluationInstance {
        ScalarEvaluationInstance::instantiate(prepared(
            name,
            Some(FunctionValueType::new(DataType::Int64, true)),
            literal,
        ))
        .unwrap()
    }
    fn bits(output: SelectedValues<'_>) -> Vec<u64> {
        assert!(output.errors().is_empty());
        let array = output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(array.null_count(), 0);
        for value in array.values() {
            assert!(*value >= 0.0 && *value < 1.0);
        }
        array.values().iter().map(|value| value.to_bits()).collect()
    }

    #[test]
    fn constant_seeds_match_independent_locked_rng_bits_for_both_actual_owners() {
        for name in ["rand", "random"] {
            for (seed, expected) in [(0, ZERO), (1, ONE), (-1, NEGATIVE), (42, FORTY_TWO)] {
                let mut instance = instance(name, Some(FunctionLiteral::Int64(seed)));
                let array: ArrayRef = Arc::new(Int64Array::from(vec![seed]));
                let arguments = [EvaluatedArgument::Scalar(&array)];
                assert_eq!(
                    bits(
                        instance
                            .evaluate(Selection::all(6), &arguments, &Control::default())
                            .unwrap()
                    ),
                    expected
                );
            }
        }
    }

    #[test]
    fn constant_sequence_continues_across_batches_empty_calls_and_sparse_domains() {
        let mut instance = instance("rand", Some(FunctionLiteral::Int64(42)));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        let rows = [2, 8];
        let selection = Selection::try_sparse(10, &rows).unwrap();
        let first = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(first.selection(), selection);
        assert_eq!(bits(first), FORTY_TWO[..2]);
        assert!(
            bits(
                instance
                    .evaluate(Selection::all(0), &arguments, &Control::default())
                    .unwrap()
            )
            .is_empty()
        );
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(4), &arguments, &Control::default())
                    .unwrap()
            ),
            FORTY_TWO[2..]
        );
    }

    #[test]
    fn each_prepared_owner_instance_has_its_own_continuous_sequence() {
        let prepared = prepared(
            "rand",
            Some(FunctionValueType::new(DataType::Int64, true)),
            Some(FunctionLiteral::Int64(1)),
        );
        let mut left = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
        let mut right = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        assert_eq!(
            bits(
                left.evaluate(Selection::all(2), &arguments, &Control::default())
                    .unwrap()
            ),
            ONE[..2]
        );
        assert_eq!(
            bits(
                right
                    .evaluate(Selection::all(6), &arguments, &Control::default())
                    .unwrap()
            ),
            ONE
        );
        assert_eq!(
            bits(
                left.evaluate(Selection::all(4), &arguments, &Control::default())
                    .unwrap()
            ),
            ONE[2..]
        );
        assert_eq!(left.retained_bytes().unwrap(), left.retained_upper_bound());
    }

    #[test]
    fn per_row_selected_negative_null_and_repeated_seeds_take_first_samples_only() {
        let mut instance = instance("random", None);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(42),
            Some(-1),
            Some(1),
            None,
            Some(-1),
        ]));
        let arguments = [EvaluatedArgument::Column(&array)];
        let rows = [1, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let output = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(output.selection(), selection);
        assert_eq!(bits(output), [NEGATIVE[0], ZERO[0], NEGATIVE[0]]);
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(5), &arguments, &Control::default())
                    .unwrap()
            ),
            [FORTY_TWO[0], NEGATIVE[0], ONE[0], ZERO[0], NEGATIVE[0]]
        );
    }

    #[test]
    fn nonconstant_seed_results_are_invariant_under_batch_splitting() {
        let mut full = instance("rand", None);
        let seeds: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(42),
            Some(-1),
            Some(1),
            None,
            Some(-1),
        ]));
        let expected = [FORTY_TWO[0], NEGATIVE[0], ONE[0], ZERO[0], NEGATIVE[0]];
        assert_eq!(
            bits(
                full.evaluate(
                    Selection::all(5),
                    &[EvaluatedArgument::Column(&seeds)],
                    &Control::default(),
                )
                .unwrap()
            ),
            expected
        );
        let mut split = instance("rand", None);
        let mut actual = Vec::new();
        for (offset, length) in [(0, 2), (2, 1), (3, 2)] {
            let batch = seeds.slice(offset, length);
            actual.extend(bits(
                split
                    .evaluate(
                        Selection::all(length),
                        &[EvaluatedArgument::Column(&batch)],
                        &Control::default(),
                    )
                    .unwrap(),
            ));
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn scalar_broadcast_does_not_reclassify_a_nonconstant_recipe() {
        let mut instance = instance("rand", None);
        let scalar: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let arguments = [EvaluatedArgument::Scalar(&scalar)];
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(4), &arguments, &Control::default())
                    .unwrap()
            ),
            [ONE[0]; 4]
        );
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(2), &arguments, &Control::default())
                    .unwrap()
            ),
            [ONE[0]; 2]
        );
    }

    #[test]
    fn compact_seed_column_addresses_selected_ordinals() {
        let mut instance = instance("rand", None);
        let rows = [2, 9];
        let selection = Selection::try_sparse(12, &rows).unwrap();
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Int64, array, Box::default()).unwrap();
        let arguments = [EvaluatedArgument::SelectedColumn(&compact)];
        let output = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(output.selection(), selection);
        assert_eq!(bits(output), [ONE[0], ZERO[0]]);
    }

    #[test]
    fn constant_cv_nonzero_ordinal_uses_checked_value_and_retains_sequence() {
        let ty = FunctionValueType::new(DataType::Int64, true);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(42), Some(-1)]));
        // Finite test admission only. This does not author a production profile.
        let policy = ConstantPolicy {
            max_rows: 3,
            max_array_nodes: 1,
            max_logical_elements: 3,
            max_retained_buffer_bytes: 1024,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            // The shared model also admits diagnostic/header scratch.
            max_library_validation_bytes: 8192,
        };
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("rand-seed").unwrap()),
            ty,
            array.to_data(),
            policy,
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let constant = pool.value(2).unwrap();
        let arguments = [EvaluatedArgument::Constant(&constant)];
        let mut instance = instance("rand", Some(FunctionLiteral::Int64(-1)));
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(3), &arguments, &Control::default())
                    .unwrap()
            ),
            NEGATIVE[..3]
        );
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(3), &arguments, &Control::default())
                    .unwrap()
            ),
            NEGATIVE[3..]
        );
        assert!(Arc::ptr_eq(constant.pool().array(), pool.array()));
    }

    #[test]
    fn constant_null_seed_has_zero_sequence_without_null_output() {
        let mut instance = instance("rand", Some(FunctionLiteral::Null));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(0), None]));
        let arguments = [EvaluatedArgument::Column(&array)];
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(3), &arguments, &Control::default())
                    .unwrap()
            ),
            ZERO[..3]
        );
    }

    #[test]
    fn runtime_constant_mismatch_is_outer_failure_and_forbids_replay() {
        let mut instance = instance("rand", Some(FunctionLiteral::Int64(1)));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2]));
        let arguments = [EvaluatedArgument::Column(&array)];
        assert!(matches!(
            instance.evaluate(Selection::all(3), &arguments, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert_eq!(
            instance
                .evaluate(Selection::all(3), &arguments, &Control::default())
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
    }

    #[test]
    fn constant_consistency_checks_only_actual_selected_seeds() {
        let mut instance = instance("rand", Some(FunctionLiteral::Int64(1)));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![999, 1, -1, 1]));
        let arguments = [EvaluatedArgument::Column(&array)];
        let rows = [1, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        assert_eq!(
            bits(
                instance
                    .evaluate(selection, &arguments, &Control::default())
                    .unwrap()
            ),
            ONE[..2]
        );
    }

    #[test]
    fn empty_domain_skips_body_and_first_nonempty_domain_starts_sequence() {
        let mut instance = instance("rand", Some(FunctionLiteral::Int64(42)));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        let control = Control::default();
        assert!(
            bits(
                instance
                    .evaluate(Selection::all(0), &arguments, &control)
                    .unwrap()
            )
            .is_empty()
        );
        assert_eq!(
            control.calls().iter().filter(|units| **units == 0).count(),
            2
        );
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(6), &arguments, &Control::default())
                    .unwrap()
            ),
            FORTY_TWO
        );
    }

    #[test]
    fn unseeded_actual_owner_outputs_nonnull_range_and_independent_inline_state() {
        let prepared = prepared("random", None, None);
        let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(8), &[], &Control::default())
                    .unwrap()
            )
            .len(),
            8
        );
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(3), &[], &Control::default())
                    .unwrap()
            )
            .len(),
            3
        );
        for recipe in [
            SeedRecipe::Unseeded,
            SeedRecipe::Constant(1),
            SeedRecipe::PerRow,
        ] {
            let instance = RandInstance::new(recipe);
            assert!(
                instance.sequence.is_none(),
                "creation must not acquire entropy or initialize RNG"
            );
            assert_eq!(
                instance.retained_bytes(),
                std::mem::size_of::<RandInstance>()
            );
        }
    }

    #[test]
    fn entry_body_256_tail_and_publication_control_failures_preserve_category_and_latch() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        let selection = Selection::all(320);
        let mut baseline = instance("rand", Some(FunctionLiteral::Int64(1)));
        let control = Control::default();
        baseline.evaluate(selection, &arguments, &control).unwrap();
        let calls = control.calls();
        let body_entry = calls
            .iter()
            .enumerate()
            .filter(|(_, units)| **units == 0)
            .nth(2)
            .unwrap()
            .0;
        let interior = calls.iter().position(|units| *units == 256).unwrap();
        assert!(body_entry < interior);
        assert_eq!(calls[interior + 1], 64);
        // The first pair belongs to frozen-seed validation; the second pair
        // belongs to the real sampling loop (255 samples precede its 256 check).
        // Refuse both pairs so preflight coverage cannot stand in for mutation.
        assert_eq!(
            calls
                .iter()
                .copied()
                .filter(|units| *units >= 64)
                .collect::<Vec<_>>(),
            [256, 64, 256, 64]
        );
        let mut refusal_indices = vec![0, body_entry, calls.len() - 1];
        for (index, units) in calls.iter().enumerate() {
            if *units == 256 {
                assert_eq!(calls[index + 1], 64);
                refusal_indices.extend([index, index + 1]);
            }
        }
        assert_eq!(
            calls.iter().filter(|units| **units == 256).count(),
            2,
            "constant consistency and actual sampling must both observe their own rows"
        );
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for at in refusal_indices.iter().copied() {
                let mut instance = instance("rand", Some(FunctionLiteral::Int64(1)));
                let control = Control::refusing(at, error.clone());
                assert_eq!(
                    instance
                        .evaluate(selection, &arguments, &control)
                        .unwrap_err(),
                    error
                );
                assert_eq!(control.calls().len(), at + 1);
                assert_eq!(
                    instance
                        .evaluate(selection, &arguments, &Control::default())
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
            }
        }
    }

    #[test]
    fn capacity_overflow_refuses_before_entropy_or_first_sample() {
        let mut unseeded =
            ScalarEvaluationInstance::instantiate(prepared("rand", None, None)).unwrap();
        let control = Control::default();
        assert_eq!(
            unseeded
                .evaluate(Selection::all(usize::MAX), &[], &control)
                .unwrap_err(),
            KernelFailure::ResourceExhausted
        );
        assert_eq!(control.calls(), [0, 0]);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let arguments = [EvaluatedArgument::Scalar(&array)];
        let mut seeded = instance("rand", Some(FunctionLiteral::Int64(1)));
        let control = Control::default();
        assert_eq!(
            seeded
                .evaluate(Selection::all(usize::MAX), &arguments, &control)
                .unwrap_err(),
            KernelFailure::ResourceExhausted
        );
        assert!(control.calls().iter().all(|units| *units < 256));
        assert_eq!(
            control.calls().iter().filter(|units| **units == 0).count(),
            3
        );
    }
}
