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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RewritePhase {
    LogicalNormalize,
    StructuralRewrite,
    SemanticRewrite,
    Validation,
}

impl RewritePhase {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LogicalNormalize => "LogicalNormalize",
            Self::StructuralRewrite => "StructuralRewrite",
            Self::SemanticRewrite => "SemanticRewrite",
            Self::Validation => "Validation",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_names_are_stable() {
        assert_eq!(RewritePhase::LogicalNormalize.as_str(), "LogicalNormalize");
        assert_eq!(
            RewritePhase::StructuralRewrite.as_str(),
            "StructuralRewrite"
        );
        assert_eq!(RewritePhase::SemanticRewrite.as_str(), "SemanticRewrite");
        assert_eq!(RewritePhase::Validation.as_str(), "Validation");
    }
}
