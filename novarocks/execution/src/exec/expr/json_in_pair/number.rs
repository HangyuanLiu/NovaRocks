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
//! Incremental serde_json 1.0.150 default-number policy (without float_roundtrip).
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum JsonNumber {
    Positive(u64),
    Negative(i64),
    Float(f64),
}
#[derive(Clone, Copy)]
enum State {
    Start,
    First,
    Integer,
    Long,
    FractionFirst,
    Fraction,
    FractionOverflow,
    ExponentSign,
    ExponentFirst,
    Exponent,
    ExponentOverflow,
    Convert,
    Done,
}
pub(super) enum NumberStep {
    Consumed,
    Progress,
    Done(JsonNumber),
    Invalid,
}
pub(super) struct NumberCursor {
    state: State,
    positive: bool,
    zero_first: bool,
    significand: u64,
    exponent: i32,
    explicit_exponent: i32,
    exponent_positive: bool,
    value: f64,
}
impl NumberCursor {
    pub(super) fn new() -> Self {
        Self {
            state: State::Start,
            positive: true,
            zero_first: false,
            significand: 0,
            exponent: 0,
            explicit_exponent: 0,
            exponent_positive: true,
            value: 0.0,
        }
    }
    fn convert(&mut self) -> NumberStep {
        self.value = self.significand as f64;
        self.state = State::Convert;
        NumberStep::Progress
    }
    pub(super) fn step(&mut self, byte: Option<u8>) -> NumberStep {
        let digit = byte.filter(u8::is_ascii_digit).map(|b| i32::from(b - b'0'));
        match self.state {
            State::Start => {
                self.state = State::First;
                if byte == Some(b'-') {
                    self.positive = false;
                    NumberStep::Consumed
                } else {
                    NumberStep::Progress
                }
            }
            State::First => match digit {
                Some(d) => {
                    self.significand = d as u64;
                    self.zero_first = d == 0;
                    self.state = State::Integer;
                    NumberStep::Consumed
                }
                None => NumberStep::Invalid,
            },
            State::Integer => {
                if let Some(d) = digit {
                    if self.zero_first {
                        return NumberStep::Invalid;
                    }
                    match self
                        .significand
                        .checked_mul(10)
                        .and_then(|v| v.checked_add(d as u64))
                    {
                        Some(v) => {
                            self.significand = v;
                            NumberStep::Consumed
                        }
                        None => {
                            self.state = State::Long;
                            NumberStep::Progress
                        }
                    }
                } else if byte == Some(b'.') {
                    self.state = State::FractionFirst;
                    NumberStep::Consumed
                } else if matches!(byte, Some(b'e' | b'E')) {
                    self.state = State::ExponentSign;
                    NumberStep::Consumed
                } else if self.positive {
                    self.state = State::Done;
                    NumberStep::Done(JsonNumber::Positive(self.significand))
                } else {
                    let negative = (self.significand as i64).wrapping_neg();
                    self.state = State::Done;
                    if negative < 0 {
                        NumberStep::Done(JsonNumber::Negative(negative))
                    } else {
                        NumberStep::Done(JsonNumber::Float(-(self.significand as f64)))
                    }
                }
            }
            State::Long => {
                if digit.is_some() {
                    self.exponent = self.exponent.saturating_add(1);
                    NumberStep::Consumed
                } else if byte == Some(b'.') {
                    self.state = State::FractionFirst;
                    NumberStep::Consumed
                } else if matches!(byte, Some(b'e' | b'E')) {
                    self.state = State::ExponentSign;
                    NumberStep::Consumed
                } else {
                    self.convert()
                }
            }
            State::FractionFirst => {
                if digit.is_none() {
                    return NumberStep::Invalid;
                }
                self.state = State::Fraction;
                NumberStep::Progress
            }
            State::Fraction => {
                if let Some(d) = digit {
                    match self
                        .significand
                        .checked_mul(10)
                        .and_then(|v| v.checked_add(d as u64))
                    {
                        Some(v) => {
                            self.significand = v;
                            self.exponent = self.exponent.saturating_sub(1);
                            NumberStep::Consumed
                        }
                        None => {
                            self.state = State::FractionOverflow;
                            NumberStep::Progress
                        }
                    }
                } else if matches!(byte, Some(b'e' | b'E')) {
                    self.state = State::ExponentSign;
                    NumberStep::Consumed
                } else {
                    self.convert()
                }
            }
            State::FractionOverflow => {
                if digit.is_some() {
                    NumberStep::Consumed
                } else if matches!(byte, Some(b'e' | b'E')) {
                    self.state = State::ExponentSign;
                    NumberStep::Consumed
                } else {
                    self.convert()
                }
            }
            State::ExponentSign => {
                self.state = State::ExponentFirst;
                if matches!(byte, Some(b'+' | b'-')) {
                    self.exponent_positive = byte == Some(b'+');
                    NumberStep::Consumed
                } else {
                    NumberStep::Progress
                }
            }
            State::ExponentFirst => match digit {
                Some(d) => {
                    self.explicit_exponent = d;
                    self.state = State::Exponent;
                    NumberStep::Consumed
                }
                None => NumberStep::Invalid,
            },
            State::Exponent => {
                if let Some(d) = digit {
                    if let Some(v) = self
                        .explicit_exponent
                        .checked_mul(10)
                        .and_then(|v| v.checked_add(d))
                    {
                        self.explicit_exponent = v;
                        NumberStep::Consumed
                    } else if self.significand != 0 && self.exponent_positive {
                        NumberStep::Invalid
                    } else {
                        self.state = State::ExponentOverflow;
                        NumberStep::Consumed
                    }
                } else {
                    self.exponent = if self.exponent_positive {
                        self.exponent.saturating_add(self.explicit_exponent)
                    } else {
                        self.exponent.saturating_sub(self.explicit_exponent)
                    };
                    self.convert()
                }
            }
            State::ExponentOverflow => {
                if digit.is_some() {
                    NumberStep::Consumed
                } else {
                    self.state = State::Done;
                    NumberStep::Done(JsonNumber::Float(if self.positive { 0.0 } else { -0.0 }))
                }
            }
            State::Convert => {
                if let Some(power) = POW10.get(self.exponent.wrapping_abs() as usize) {
                    if self.exponent >= 0 {
                        self.value *= power;
                        if self.value.is_infinite() {
                            return NumberStep::Invalid;
                        }
                    } else {
                        self.value /= power;
                    }
                    self.state = State::Done;
                    NumberStep::Done(JsonNumber::Float(if self.positive {
                        self.value
                    } else {
                        -self.value
                    }))
                } else if self.value == 0.0 {
                    self.state = State::Done;
                    NumberStep::Done(JsonNumber::Float(if self.positive { 0.0 } else { -0.0 }))
                } else if self.exponent >= 0 {
                    NumberStep::Invalid
                } else {
                    self.value /= 1e308;
                    self.exponent += 308;
                    NumberStep::Progress
                }
            }
            State::Done => NumberStep::Invalid,
        }
    }
}
// Literal powers have exactly the same rounding as serde_json's default table.
const POW10: [f64; 309] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31, 1e32,
    1e33, 1e34, 1e35, 1e36, 1e37, 1e38, 1e39, 1e40, 1e41, 1e42, 1e43, 1e44, 1e45, 1e46, 1e47, 1e48,
    1e49, 1e50, 1e51, 1e52, 1e53, 1e54, 1e55, 1e56, 1e57, 1e58, 1e59, 1e60, 1e61, 1e62, 1e63, 1e64,
    1e65, 1e66, 1e67, 1e68, 1e69, 1e70, 1e71, 1e72, 1e73, 1e74, 1e75, 1e76, 1e77, 1e78, 1e79, 1e80,
    1e81, 1e82, 1e83, 1e84, 1e85, 1e86, 1e87, 1e88, 1e89, 1e90, 1e91, 1e92, 1e93, 1e94, 1e95, 1e96,
    1e97, 1e98, 1e99, 1e100, 1e101, 1e102, 1e103, 1e104, 1e105, 1e106, 1e107, 1e108, 1e109, 1e110,
    1e111, 1e112, 1e113, 1e114, 1e115, 1e116, 1e117, 1e118, 1e119, 1e120, 1e121, 1e122, 1e123,
    1e124, 1e125, 1e126, 1e127, 1e128, 1e129, 1e130, 1e131, 1e132, 1e133, 1e134, 1e135, 1e136,
    1e137, 1e138, 1e139, 1e140, 1e141, 1e142, 1e143, 1e144, 1e145, 1e146, 1e147, 1e148, 1e149,
    1e150, 1e151, 1e152, 1e153, 1e154, 1e155, 1e156, 1e157, 1e158, 1e159, 1e160, 1e161, 1e162,
    1e163, 1e164, 1e165, 1e166, 1e167, 1e168, 1e169, 1e170, 1e171, 1e172, 1e173, 1e174, 1e175,
    1e176, 1e177, 1e178, 1e179, 1e180, 1e181, 1e182, 1e183, 1e184, 1e185, 1e186, 1e187, 1e188,
    1e189, 1e190, 1e191, 1e192, 1e193, 1e194, 1e195, 1e196, 1e197, 1e198, 1e199, 1e200, 1e201,
    1e202, 1e203, 1e204, 1e205, 1e206, 1e207, 1e208, 1e209, 1e210, 1e211, 1e212, 1e213, 1e214,
    1e215, 1e216, 1e217, 1e218, 1e219, 1e220, 1e221, 1e222, 1e223, 1e224, 1e225, 1e226, 1e227,
    1e228, 1e229, 1e230, 1e231, 1e232, 1e233, 1e234, 1e235, 1e236, 1e237, 1e238, 1e239, 1e240,
    1e241, 1e242, 1e243, 1e244, 1e245, 1e246, 1e247, 1e248, 1e249, 1e250, 1e251, 1e252, 1e253,
    1e254, 1e255, 1e256, 1e257, 1e258, 1e259, 1e260, 1e261, 1e262, 1e263, 1e264, 1e265, 1e266,
    1e267, 1e268, 1e269, 1e270, 1e271, 1e272, 1e273, 1e274, 1e275, 1e276, 1e277, 1e278, 1e279,
    1e280, 1e281, 1e282, 1e283, 1e284, 1e285, 1e286, 1e287, 1e288, 1e289, 1e290, 1e291, 1e292,
    1e293, 1e294, 1e295, 1e296, 1e297, 1e298, 1e299, 1e300, 1e301, 1e302, 1e303, 1e304, 1e305,
    1e306, 1e307, 1e308,
];
