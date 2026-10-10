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

//! Immutable original request data and separately authored emission values.
//! This owner neither interprets the values nor certifies their provenance.

use novarocks_functions::ConstantValue;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use crate::binding::CapturedLogicalCallArguments;

pub(crate) struct DerivedVariantSource {
    captured: CapturedLogicalCallArguments,
    canonical_path: ConstantValue,
    type_literal: ConstantValue,
}

impl DerivedVariantSource {
    /// Move already-authored data without cloning its types or pool backing.
    /// The producer retains responsibility for grammar, values, source
    /// correspondence and admission of the objects supplied to this owner.
    pub(crate) fn new_observed(
        captured: CapturedLogicalCallArguments,
        canonical_path: ConstantValue,
        type_literal: ConstantValue,
        control: &dyn PureCompileControl,
    ) -> Result<Self, CompileControlError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let source = Self {
            captured,
            canonical_path,
            type_literal,
        };
        // The three original owned fields have moved into this owner. No
        // request allocation, value scan or internal Arc clone occurs here.
        work.step()?;
        work.step()?;
        work.step()?;
        work.finish()?;
        Ok(source)
    }

    pub(crate) const fn captured(&self) -> &CapturedLogicalCallArguments {
        &self.captured
    }

    pub(crate) const fn canonical_path(&self) -> &ConstantValue {
        &self.canonical_path
    }

    pub(crate) const fn type_literal(&self) -> &ConstantValue {
        &self.type_literal
    }
}
