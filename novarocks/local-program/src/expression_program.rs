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

//! Immutable checked expressions shared by native and compiled task preparation.
use crate::creation::{ProgramCreation, ProgramCreationFailure};
use crate::primitives::CreatedPrimitiveMap;
use crate::{
    LocalProgramGraph, ProgramCallSite, ProgramComparisonSite, ProgramLexicalBindings,
    ProgramPrimitiveError, ProgramStateTemplate,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, ValueSchemaCreationFactory,
    ValueSchemaCreationScope, ValueTypeStorageLifetime,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct ExpressionProgram {
    checked: ProgramLexicalBindings,
    native_negate:
        CreatedPrimitiveMap<crate::ProgramUseRef, novarocks_functions::PreparedNativeNegateRecipe>,
    native_bitnot:
        CreatedPrimitiveMap<crate::ProgramUseRef, novarocks_functions::PreparedNativeBitNotRecipe>,
    native_inlist:
        CreatedPrimitiveMap<crate::ProgramUseRef, novarocks_functions::PreparedNativeInListRecipe>,
    native_like:
        CreatedPrimitiveMap<crate::ProgramUseRef, novarocks_functions::PreparedNativeLikeRecipe>,
    arithmetic:
        CreatedPrimitiveMap<crate::ProgramUseRef, novarocks_functions::PreparedArithmeticRecipe>,
    casts: CreatedPrimitiveMap<crate::ProgramUseRef, novarocks_functions::PreparedCastRecipe>,
    comparisons:
        CreatedPrimitiveMap<ProgramComparisonSite, novarocks_functions::PreparedComparisonRecipe>,
    null_safe_comparisons: CreatedPrimitiveMap<
        crate::ProgramUseRef,
        novarocks_functions::PreparedNullSafeComparisonRecipe,
    >,
    // Release this Arc's structure responsibility after its children.
    _storage: ValueTypeStorageLifetime,
}
impl ExpressionProgram {
    pub fn try_new(
        checked: ProgramLexicalBindings,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self>, ProgramPrimitiveError> {
        Self::try_new_core::<std::convert::Infallible, crate::creation::NoFactory>(
            checked, None, None, control,
        )
        .map_err(crate::primitives::pure_primitive_failure)
    }
    pub fn try_new_with_creator_in<H, F: ValueSchemaCreationFactory<H>>(
        checked: ProgramLexicalBindings,
        temporary: &mut dyn ValueSchemaCreationScope<H>,
        retained_factory: &F,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self>, ProgramCreationFailure<ProgramPrimitiveError, H>> {
        Self::try_new_core(checked, Some(temporary), Some(retained_factory), control)
    }
    fn try_new_core<H, F: ValueSchemaCreationFactory<H>>(
        checked: ProgramLexicalBindings,
        mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
        retained_factory: Option<&F>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self>, ProgramCreationFailure<ProgramPrimitiveError, H>> {
        // Every family keeps its original independent checkpoints and order.
        // Only IN constructs temporary candidate-reference storage.
        let native_like = crate::primitives::compile_native_like_with_factory(
            &checked,
            None,
            retained_factory,
            control,
        )?;
        let native_inlist = crate::primitives::compile_native_inlist_with_factory(
            &checked,
            temporary.take(),
            retained_factory,
            control,
        )?;
        let native_negate = crate::primitives::compile_native_negate_with_factory(
            &checked,
            None,
            retained_factory,
            control,
        )?;
        let native_bitnot = crate::primitives::compile_native_bitnot_with_factory(
            &checked,
            None,
            retained_factory,
            control,
        )?;
        let arithmetic = crate::primitives::compile_arithmetic_with_factory(
            &checked,
            None,
            retained_factory,
            control,
        )?;
        let casts = crate::primitives::compile_casts_with_factory(
            &checked,
            None,
            retained_factory,
            control,
        )?;
        let comparisons = crate::primitives::compile_comparisons_with_factory(
            &checked,
            None,
            retained_factory,
            control,
        )?;
        let null_safe_comparisons = crate::primitives::compile_null_safe_comparisons_with_factory(
            &checked,
            None,
            retained_factory,
            control,
        )?;
        let storage = match retained_factory {
            Some(factory) => {
                // This is the real final Arc operation's own observation,
                // after the eight original family constructors have succeeded.
                let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
                let mut scope = factory
                    .begin_scope()
                    .map_err(ProgramCreationFailure::Creator)?;
                let mut creation = ProgramCreation {
                    creator: Some(&mut scope),
                };
                creation.before_arc::<ProgramPrimitiveError, Self>(&mut work)?;
                let storage = creation.storage();
                work.finish()?;
                storage
            }
            None => ValueTypeStorageLifetime::pure(),
        };
        Ok(Arc::new(Self {
            checked,
            _storage: storage,
            native_like,
            native_inlist,
            native_negate,
            native_bitnot,
            arithmetic,
            casts,
            comparisons,
            null_safe_comparisons,
        }))
    }
    pub const fn checked(&self) -> &ProgramLexicalBindings {
        &self.checked
    }
    pub fn graph(&self) -> &LocalProgramGraph {
        self.checked
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot()
            .program()
    }
    pub fn comparison_recipe(
        &self,
        site: ProgramComparisonSite,
    ) -> Option<&novarocks_functions::PreparedComparisonRecipe> {
        self.comparisons.get(&site)
    }
    pub fn null_safe_comparison_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNullSafeComparisonRecipe> {
        self.null_safe_comparisons.get(&site)
    }
    pub fn native_like_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNativeLikeRecipe> {
        self.native_like.get(&site)
    }
    pub fn native_inlist_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNativeInListRecipe> {
        self.native_inlist.get(&site)
    }
    pub fn native_negate_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNativeNegateRecipe> {
        self.native_negate.get(&site)
    }
    pub fn native_bitnot_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNativeBitNotRecipe> {
        self.native_bitnot.get(&site)
    }
    pub fn arithmetic_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedArithmeticRecipe> {
        self.arithmetic.get(&site)
    }
    pub fn cast_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedCastRecipe> {
        self.casts.get(&site)
    }
    /// Borrow the exact checked implementation's lifecycle; never rebuild a
    /// second state declaration from a name or legacy expression tag.
    pub fn state_template(&self, site: ProgramCallSite) -> Option<ProgramStateTemplate<'_>> {
        self.checked
            .channels()
            .expressions()
            .resolved_calls()
            .calls()
            .get(&site)
            .map(|call| call.state_template())
    }
}
