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
//! Nominal source shape, domain and call-effect contract fixtures.
use crate::{
    ArgumentControl, ControlShape, EvaluationDemand, ExpressionControlFlowError, GuardKind,
    TemporalSourceKind as Kind, TemporalSourceShape as Shape, control_argument_semantics,
};
#[test]
fn temporal_source_controls_match_only_the_exact_nominal_family() {
    for shape in [
        Shape::FormatOrdinary,
        Shape::FormatUtf8Override,
        Shape::SecondsDirect,
        Shape::SecondsCastString,
        Shape::SecondsCastOther,
        Shape::SecondsRoundtrip,
    ] {
        assert!(
            ArgumentControl::TemporalSource(shape.kind())
                .matches_scalar_shape(ControlShape::TemporalSource(shape))
        );
        let other = if shape.kind() == Kind::TimeFormat {
            Kind::TimeToSec
        } else {
            Kind::TimeFormat
        };
        assert!(
            !ArgumentControl::TemporalSource(other)
                .matches_scalar_shape(ControlShape::TemporalSource(shape))
        );
        assert!(!ArgumentControl::Eager.matches_scalar_shape(ControlShape::TemporalSource(shape)));
    }
}
#[test]
fn temporal_source_edges_always_require_value_and_exact_phase_guards() {
    for shape in [
        Shape::FormatOrdinary,
        Shape::FormatUtf8Override,
        Shape::SecondsDirect,
        Shape::SecondsCastString,
        Shape::SecondsCastOther,
        Shape::SecondsRoundtrip,
    ] {
        for ordinal in 0..shape.source_count() {
            let expected = if ordinal == 0 {
                None
            } else if shape == Shape::SecondsCastOther && ordinal == 2 {
                Some(GuardKind::TemporalInvocationNull)
            } else {
                Some(GuardKind::TemporalAfterSource {
                    ordinal: (ordinal - 1) as u32,
                })
            };
            for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
                assert_eq!(
                    control_argument_semantics(
                        ControlShape::TemporalSource(shape),
                        shape.source_count(),
                        ordinal,
                        demand
                    ),
                    Ok((EvaluationDemand::Value, expected))
                );
            }
        }
        assert_eq!(
            control_argument_semantics(
                ControlShape::TemporalSource(shape),
                shape.source_count() + 1,
                0,
                EvaluationDemand::Value
            ),
            Err(ExpressionControlFlowError::InvalidControlShape)
        );
    }
}
