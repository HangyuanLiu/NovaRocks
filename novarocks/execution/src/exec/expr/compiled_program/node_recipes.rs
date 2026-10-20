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

//! Borrowed recipes for one expression node, independent of program traversal.
use novarocks_functions::*;
use novarocks_local_program::{
    ExpressionProgram, ProgramCallSite, ProgramComparisonSite, ProgramUseRef,
};

#[derive(Clone, Copy)]
pub(super) enum ComparisonPosition {
    Binary,
    BetweenLower,
    BetweenUpper,
    CaseWhen(u32),
}

pub(super) trait NodeRecipes {
    fn comparison_recipe(&self, position: ComparisonPosition) -> Option<&PreparedComparisonRecipe>;
    fn null_safe_comparison_recipe(&self) -> Option<&PreparedNullSafeComparisonRecipe>;
    fn native_like_recipe(&self) -> Option<&PreparedNativeLikeRecipe>;
    fn native_inlist_recipe(&self) -> Option<&PreparedNativeInListRecipe>;
    fn native_negate_recipe(&self) -> Option<&PreparedNativeNegateRecipe>;
    fn native_bitnot_recipe(&self) -> Option<&PreparedNativeBitNotRecipe>;
    fn arithmetic_recipe(&self) -> Option<&PreparedArithmeticRecipe>;
    fn cast_recipe(&self) -> Option<&PreparedCastRecipe>;
    fn prepared_call(&self) -> Option<&PreparedPureKernel>;
}

pub(super) struct ProgramNodeRecipes<'a> {
    pub program: &'a ExpressionProgram,
    pub occurrence: ProgramUseRef,
}
impl NodeRecipes for ProgramNodeRecipes<'_> {
    fn comparison_recipe(&self, position: ComparisonPosition) -> Option<&PreparedComparisonRecipe> {
        let site = match position {
            ComparisonPosition::Binary => ProgramComparisonSite::Binary(self.occurrence),
            ComparisonPosition::BetweenLower => {
                ProgramComparisonSite::BetweenLower(self.occurrence)
            }
            ComparisonPosition::BetweenUpper => {
                ProgramComparisonSite::BetweenUpper(self.occurrence)
            }
            ComparisonPosition::CaseWhen(arm) => ProgramComparisonSite::CaseWhen {
                occurrence: self.occurrence,
                arm,
            },
        };
        self.program.comparison_recipe(site)
    }
    fn null_safe_comparison_recipe(&self) -> Option<&PreparedNullSafeComparisonRecipe> {
        self.program.null_safe_comparison_recipe(self.occurrence)
    }
    fn native_like_recipe(&self) -> Option<&PreparedNativeLikeRecipe> {
        self.program.native_like_recipe(self.occurrence)
    }
    fn native_inlist_recipe(&self) -> Option<&PreparedNativeInListRecipe> {
        self.program.native_inlist_recipe(self.occurrence)
    }
    fn native_negate_recipe(&self) -> Option<&PreparedNativeNegateRecipe> {
        self.program.native_negate_recipe(self.occurrence)
    }
    fn native_bitnot_recipe(&self) -> Option<&PreparedNativeBitNotRecipe> {
        self.program.native_bitnot_recipe(self.occurrence)
    }
    fn arithmetic_recipe(&self) -> Option<&PreparedArithmeticRecipe> {
        self.program.arithmetic_recipe(self.occurrence)
    }
    fn cast_recipe(&self) -> Option<&PreparedCastRecipe> {
        self.program.cast_recipe(self.occurrence)
    }
    fn prepared_call(&self) -> Option<&PreparedPureKernel> {
        self.program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .calls()
            .get(&ProgramCallSite::Expression(self.occurrence))
            .map(|call| call.specialization().prepared())
    }
}
