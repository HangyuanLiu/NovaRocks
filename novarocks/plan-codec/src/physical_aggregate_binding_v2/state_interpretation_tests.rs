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

use super::*;
use novarocks_type_contract::{AggregateStateInterpretation as I, AggregateStateOrderKey as K};
use std::sync::Mutex;
#[derive(Default)]
struct Control {
    units: Mutex<Vec<u32>>,
    refuse: Option<CompileControlError>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        self.units.lock().unwrap().push(units);
        self.refuse.map_or(Ok(()), Err)
    }
}
#[test]
fn original_codec_preserves_explicit_plain_and_every_state_order_bit() {
    for distinct in [false, true] {
        for keys in [
            vec![],
            vec![K {
                ascending: false,
                nulls_first: false,
            }],
            vec![
                K {
                    ascending: true,
                    nulls_first: true,
                },
                K {
                    ascending: false,
                    nulls_first: true,
                },
            ],
        ] {
            let source = I {
                distinct,
                order_keys: keys.into_boxed_slice(),
            };
            let control = Control::default();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let raw = encode_state_interpretation(&source, &mut work).unwrap();
            assert_eq!(raw.distinct, Some(distinct));
            let result = decode_state_interpretation(&raw, &mut work).unwrap();
            assert_eq!(source, result);
            work.finish().unwrap();
        }
    }
}
#[test]
fn original_codec_rejects_missing_distinct_direction_or_null_placement() {
    let invalids = [
        wire::AggregateStateInterpretation {
            distinct: None,
            order_keys: vec![],
        },
        wire::AggregateStateInterpretation {
            distinct: Some(false),
            order_keys: vec![wire::AggregateStateOrderKey {
                ascending: None,
                nulls_first: Some(false),
            }],
        },
        wire::AggregateStateInterpretation {
            distinct: Some(true),
            order_keys: vec![wire::AggregateStateOrderKey {
                ascending: Some(true),
                nulls_first: None,
            }],
        },
    ];
    for raw in invalids {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        assert!(matches!(
            decode_state_interpretation(&raw, &mut work),
            Err(BindingCodecError::InvalidShape(_))
        ));
    }
}
#[test]
fn receipt_codec_keeps_control_cause_and_bounded_observation_on_long_keys() {
    let source = I {
        distinct: true,
        order_keys: vec![
            K {
                ascending: true,
                nulls_first: false
            };
            1025
        ]
        .into_boxed_slice(),
    };
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    encode_state_interpretation(&source, &mut work).unwrap();
    work.finish().unwrap();
    assert!(
        control
            .units
            .lock()
            .unwrap()
            .iter()
            .any(|units| *units == 256)
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            refuse: Some(cause),
            ..Default::default()
        };
        assert!(
            matches!(CompileCheckpoints::try_new(&control, CompilePhase::Encode), Err(actual) if actual == cause)
        );
        assert_eq!(control.units.lock().unwrap().len(), 1);
    }
}
