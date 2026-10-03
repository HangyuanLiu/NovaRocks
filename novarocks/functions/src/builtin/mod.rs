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

//! Pure builtin signature resolution and source-audited declaration inventory.
//! SQL syntax and the application catalogue stay with their respective owners.

pub mod intrinsic;
pub mod registry;
pub mod resolver;
pub mod signature;

pub mod catalogue;

pub mod value_conversion;
mod value_conversion_kernel;
mod value_conversion_owner;

mod binding_control;

#[cfg(test)]
mod binding_control_tests;

mod abs;
mod abs_owner;
mod bit_shift;
mod bit_shift_owner;
mod bitwise;
mod bitwise_owner;
mod calendar_day_number;
mod calendar_day_number_owner;
mod calendar_diff;
mod calendar_diff_owner;
mod calendar_parts;
mod calendar_parts_owner;
mod control_owner;
mod crc32;
mod crc32_owner;
mod date;
mod date_owner;
mod dround;
mod dround_owner;
mod numeric_binary;
mod numeric_binary_owner;
mod numeric_elementary;
mod numeric_elementary_owner;
mod numeric_mod;
mod numeric_mod_owner;
mod numeric_unary;
mod numeric_unary_owner;
mod rand;
mod rand_owner;
mod round;
mod round_cast;
mod round_cast_float_text;
mod round_cast_text;
mod round_owner;
mod rounding_binding;
mod string_case;
mod string_case_owner;
mod string_concat;
mod string_concat_owner;
mod string_find_in_set;
mod string_find_in_set_owner;
mod string_initcap;
mod string_initcap_owner;
mod string_left_right;
mod string_left_right_owner;
mod string_locate;
mod string_locate_owner;
mod string_measure;
mod string_measure_owner;
mod string_pad;
mod string_pad_owner;
mod string_repeat;
mod string_repeat_owner;
mod string_replace;
mod string_replace_owner;
mod string_reverse;
mod string_reverse_owner;
mod string_split_part;
mod string_split_part_owner;
mod string_substring;
mod string_substring_index;
mod string_substring_index_owner;
mod string_substring_owner;
mod string_translate;
mod string_translate_owner;
mod string_trim;
mod string_trim_owner;
mod string_url_decode;
mod string_url_decode_owner;
mod string_url_encode;
mod string_url_encode_owner;
mod truncate;
mod truncate_owner;
