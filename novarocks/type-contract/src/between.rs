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

//! One neutral expansion of the original native BETWEEN operation. It creates
//! no expression identities and contains no comparison or Boolean algorithm.
use crate::{ComparisonOperator, ControlShape};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BetweenSourceRole {
    Operand,
    Lower,
    Upper,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeBetweenPlan {
    negated: bool,
}
impl NativeBetweenPlan {
    pub const fn new(negated: bool) -> Self {
        Self { negated }
    }
    pub const fn negated(self) -> bool {
        self.negated
    }
    pub const fn sources(self) -> [BetweenSourceRole; 4] {
        [
            BetweenSourceRole::Operand,
            BetweenSourceRole::Lower,
            BetweenSourceRole::Operand,
            BetweenSourceRole::Upper,
        ]
    }
    pub const fn lower(self) -> ComparisonOperator {
        if self.negated {
            ComparisonOperator::Lt
        } else {
            ComparisonOperator::Ge
        }
    }
    pub const fn upper(self) -> ComparisonOperator {
        if self.negated {
            ComparisonOperator::Gt
        } else {
            ComparisonOperator::Le
        }
    }
    pub const fn connective(self) -> ControlShape {
        if self.negated {
            ControlShape::Disjunction
        } else {
            ControlShape::Conjunction
        }
    }
}
