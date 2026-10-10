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

// The integration owner supplies the original binding and typed DML errors.
use novarocks_parser::ast::Expr;
use std::mem::size_of;

use super::borrowed_value_footprint::{Failure, ValueFootprint};

// Reuse the original admitted construction profile; never create a budget.
use crate::query_execution::internal_result_cpu::INTERNAL_PEAK_BYTES as ORIGINAL_INTERNAL_PEAK;
type Result<T> = std::result::Result<T, Failure>;
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(Failure::ResourceExhausted)
}
fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b).ok_or(Failure::ResourceExhausted)
}

// Concrete simultaneously live copies: FE's prepared query, SqlCompiler's
// ParsedQuery clone, the analyzer's normalization clone, and the prepass
// unroller clone. Once analysis returns, semantic detectors use the third
// slot serially. General
// analyzer/optimizer graphs remain planning Work, outside this conversion.
fn current_compiler_copies_upper(one_query: u64) -> Result<u64> {
    let fe_prepared_query = one_query;
    let compiler_parsed_query = one_query;
    let analyzer_or_semantic_detector_query = one_query;
    let prepass_rewritten_query = one_query;
    add(
        add(fe_prepared_query, compiler_parsed_query)?,
        add(analyzer_or_semantic_detector_query, prepass_rewritten_query)?,
    )
}

/// All fields require a concrete original-owner receipt. Wire/content lengths
/// are not receipts. Shared source ownership is counted once physically.
/// Existing catalog/provider runtime metadata does not become Internal merely
/// because a Rust session references it.
pub struct ExistingBackingEvidence {
    // The provider's successful receipt already includes the original
    // selection, source, request and every provider-owned construction phase.
    pub session_simultaneous_upper: u64,
    pub sorted_borrowed_target_vector_upper: u64,
    pub recipe_and_small_scalars_upper: u64,
}
impl ExistingBackingEvidence {
    fn total(&self) -> Result<u64> {
        [
            self.session_simultaneous_upper,
            self.sorted_borrowed_target_vector_upper,
            self.recipe_and_small_scalars_upper,
        ]
        .into_iter()
        .try_fold(0, add)
    }
}

/// A template is produced by the pure closed skeleton layout algebra; its
/// producer counts prospective Vec/String/Box allocations without first
/// constructing a template AST. The later deep walk verifies the real graph.
/// It contains no VALUES row Expr slots: TargetAccumulator adds those cells.
/// It does contain Values.rows exact Vec<Vec<Expr>> slots and all skeleton
/// projections/joins/aliases/TypeNames. No generated version/time-travel refs.
pub struct SkeletonEvidence {
    pub ast_owned: u64,
    pub clone_owned_upper: u64,
    pub owned_target_metadata_upper: u64,
    pub rows: u64,
    pub width: u64,
    pub selection_cast_width: u64,
    pub one_row_selection_cast_type_heap: u64,
    pub constructor_transient_upper: u64,
}

fn minimum_ast(template: &SkeletonEvidence) -> Result<u64> {
    if template.selection_cast_width > template.width {
        return Err(Failure::InvalidSource("COW recipe selection width"));
    }
    // Outer row Expr slots plus each CAST's boxed child and actual TypeName.
    // Fixed marker cells may be one Expr; this is a necessary lower bound.
    let row = add(
        mul(
            add(template.width, template.selection_cast_width)?,
            size_of::<Expr>() as u64,
        )?,
        template.one_row_selection_cast_type_heap,
    )?;
    add(template.ast_owned, mul(template.rows, row)?)
}

/// Numeric accumulator only: no per-cell/row recipe allocation.
pub struct TargetAccumulator {
    own: u64,
    clone_upper: u64,
    metadata: u64,
    cells_expected: u64,
    cells_observed: u64,
    maximum_constructor_transient: u64,
    remaining_upper: u64,
}
impl TargetAccumulator {
    pub fn new(template: SkeletonEvidence, remaining_upper: u64) -> Result<Self> {
        let minimum = add(
            add(
                minimum_ast(&template)?,
                template.owned_target_metadata_upper,
            )?,
            template.constructor_transient_upper,
        )?;
        if minimum > remaining_upper {
            return Err(Failure::ResourceExhausted);
        }
        Ok(Self {
            own: template.ast_owned,
            clone_upper: template.clone_owned_upper,
            metadata: template.owned_target_metadata_upper,
            cells_expected: mul(template.rows, template.width)?,
            cells_observed: 0,
            maximum_constructor_transient: template.constructor_transient_upper,
            remaining_upper,
        })
    }

    /// The type heap comes from an exact closed TypeName allocation recipe.
    /// TypeName itself is inline in Expr::Cast and must not be counted twice.
    /// ValueFootprint includes the child Expr pointee, so the Box's pointer is
    /// already in the outer Expr slot, not another independent allocation.
    pub fn selection_cast(&mut self, value: ValueFootprint, exact_type_heap: u64) -> Result<()> {
        if self.cells_observed == self.cells_expected {
            return Err(Failure::InvalidSource("COW recipe cell count"));
        }
        let cell = add(
            add(size_of::<Expr>() as u64, exact_type_heap)?,
            value.ast_owned,
        )?;
        self.own = add(self.own, cell)?;
        // Paired builder uses exact capacities; cloning may request at most
        // this graph's exact owned layout. Actual deep-walk confirms it before
        // FE cloning; it never substitutes for the first growth preflight.
        self.clone_upper = add(self.clone_upper, cell)?;
        self.cells_observed += 1;
        if add(
            add(self.own, self.metadata)?,
            self.maximum_constructor_transient,
        )? > self.remaining_upper
        {
            return Err(Failure::ResourceExhausted);
        }
        Ok(())
    }

    /// Marker TRUE/effect and other values must use their actual AST recipe,
    /// not pretend to be a signed selection CAST with a made-up type.
    pub fn fixed_cell(&mut self, ast_owned: u64, temporary: u64) -> Result<()> {
        if self.cells_observed == self.cells_expected {
            return Err(Failure::InvalidSource("COW recipe cell count"));
        }
        if ast_owned < size_of::<Expr>() as u64 {
            return Err(Failure::InvalidSource("COW fixed cell root layout"));
        }
        self.own = add(self.own, ast_owned)?;
        self.clone_upper = add(self.clone_upper, ast_owned)?;
        self.maximum_constructor_transient = self.maximum_constructor_transient.max(temporary);
        self.cells_observed += 1;
        if add(
            add(self.own, self.metadata)?,
            self.maximum_constructor_transient,
        )? > self.remaining_upper
        {
            return Err(Failure::ResourceExhausted);
        }
        Ok(())
    }
}

pub struct Recipe {
    pub baseline_existing: u64,
    pub all_targets_ast: u64,
    pub all_targets_owned_metadata: u64,
    pub maximum_current_target_clone: u64,
    pub maximum_current_cell_transient: u64,
    pub target_count: u64,
}
impl Recipe {
    pub fn start(evidence: Option<ExistingBackingEvidence>) -> Result<Self> {
        // Missing original source/container/session evidence never means zero.
        let evidence = evidence.ok_or(Failure::InvalidSource("unproved COW original backing"))?;
        let recipe = Self {
            baseline_existing: evidence.total()?,
            all_targets_ast: 0,
            all_targets_owned_metadata: 0,
            maximum_current_target_clone: 0,
            maximum_current_cell_transient: 0,
            target_count: 0,
        };
        recipe.check()?;
        Ok(recipe)
    }

    pub fn begin_target(&self, template: SkeletonEvidence) -> Result<TargetAccumulator> {
        let minimum = minimum_ast(&template)?;
        let maximum_clone = self.maximum_current_target_clone.max(minimum);
        let transient = self
            .maximum_current_cell_transient
            .max(template.constructor_transient_upper);
        let lower_peak = add(
            add(
                add(
                    add(
                        add(self.baseline_existing, self.all_targets_owned_metadata)?,
                        template.owned_target_metadata_upper,
                    )?,
                    self.all_targets_ast,
                )?,
                minimum,
            )?,
            add(current_compiler_copies_upper(maximum_clone)?, transient)?,
        )?;
        if lower_peak > ORIGINAL_INTERNAL_PEAK {
            return Err(Failure::ResourceExhausted);
        }
        // Passing this necessary test still requires every borrowed cell's
        // full exact upper recipe and the final all-target check.
        TargetAccumulator::new(template, self.remaining_for_target()?)
    }

    pub fn remaining_for_target(&self) -> Result<u64> {
        self.check()?;
        ORIGINAL_INTERNAL_PEAK
            .checked_sub(self.peak()?)
            .ok_or(Failure::ResourceExhausted)
    }

    pub fn finish_target(&mut self, target: TargetAccumulator) -> Result<()> {
        if target.cells_observed != target.cells_expected {
            return Err(Failure::InvalidSource("incomplete COW target recipe"));
        }
        self.all_targets_ast = add(self.all_targets_ast, target.own)?;
        self.all_targets_owned_metadata = add(self.all_targets_owned_metadata, target.metadata)?;
        self.maximum_current_target_clone =
            self.maximum_current_target_clone.max(target.clone_upper);
        self.maximum_current_cell_transient = self
            .maximum_current_cell_transient
            .max(target.maximum_constructor_transient);
        self.target_count = add(self.target_count, 1)?;
        self.check()
    }

    /// Only the complete sealed target set may authorize AST construction.
    /// Consuming the accumulator prevents adding an unproved target later.
    pub fn finish_all(self, expected_targets: u64) -> Result<CheckedRecipe> {
        if self.target_count != expected_targets {
            return Err(Failure::InvalidSource("incomplete COW all-target recipe"));
        }
        self.check()?;
        Ok(CheckedRecipe { recipe: self })
    }

    /// A conservative envelope covering construction AND compilation. Every
    /// target original AST is held, plus the current FE-prepared, ParsedQuery
    /// and analyzer/semantic-detector/prepass copies. Constructor transient is retained conservatively;
    /// it is not a second pool and is not spent again by another target.
    pub fn peak(&self) -> Result<u64> {
        add(
            add(
                add(
                    add(self.baseline_existing, self.all_targets_owned_metadata)?,
                    self.all_targets_ast,
                )?,
                current_compiler_copies_upper(self.maximum_current_target_clone)?,
            )?,
            self.maximum_current_cell_transient,
        )
    }
    pub fn check(&self) -> Result<()> {
        if self.peak()? > ORIGINAL_INTERNAL_PEAK {
            Err(Failure::ResourceExhausted)
        } else {
            Ok(())
        }
    }
}

/// Numeric proof only, not a permit or binding. The private caller retains the
/// original admitted window/activity and the original immutable selection.
/// This object must not approve a different route or target set.
pub struct CheckedRecipe {
    recipe: Recipe,
}
impl CheckedRecipe {
    pub fn peak_upper(&self) -> Result<u64> {
        self.recipe.peak()
    }
    pub fn maximum_current_target_clone(&self) -> u64 {
        self.recipe.maximum_current_target_clone
    }
}
