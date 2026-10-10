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
/// Original before-child argument-count predicate. Callers supply the real
/// metadata bounds; this author does not select functions or evaluate inputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvocationArity {
    pub minimum: usize,
    pub maximum: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvocationArityFailure {
    pub minimum: usize,
    pub maximum: usize,
    pub actual: usize,
}
impl InvocationArityFailure {
    /// ONE original formatter recipe. The label is the actual metadata fact;
    /// it never selects computation or demand. Legacy uses std::fmt::format;
    /// a selected host writes these SAME arguments under its actual admission.
    pub fn with_original_message<T>(
        self,
        label: &str,
        sink: impl FnOnce(std::fmt::Arguments<'_>) -> T,
    ) -> T {
        sink(format_args!(
            "{} expects {} to {} arguments, got {}",
            label, self.minimum, self.maximum, self.actual
        ))
    }
}
impl InvocationArity {
    pub const fn failure(self, actual: usize) -> Option<InvocationArityFailure> {
        if actual < self.minimum || actual > self.maximum {
            Some(InvocationArityFailure {
                minimum: self.minimum,
                maximum: self.maximum,
                actual,
            })
        } else {
            None
        }
    }
}
/// These bounds are the original ARRAY FunctionMeta facts. Both legacy metadata
/// records and the selected owner borrow this same author.
pub const ARRAY_STRUCT_SUBFIELD_ARITY: InvocationArity = InvocationArity {
    minimum: 2,
    maximum: 2,
};
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn array_original_arity_author_preserves_zero_child_boundary() {
        for actual in [0, 1, 3, 5, usize::MAX] {
            assert_eq!(
                ARRAY_STRUCT_SUBFIELD_ARITY.failure(actual),
                Some(InvocationArityFailure {
                    minimum: 2,
                    maximum: 2,
                    actual
                })
            );
        }
        assert_eq!(ARRAY_STRUCT_SUBFIELD_ARITY.failure(2), None);
    }
    #[test]
    fn array_original_arity_author_accepts_original_inclusive_range() {
        let original = InvocationArity {
            minimum: 2,
            maximum: 3,
        };
        for actual in 0..5 {
            assert_eq!(original.failure(actual).is_some(), actual < 2 || actual > 3);
        }
    }
}
