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

pub(crate) mod binding_control;

#[cfg(test)]
mod binding_control_tests;

mod abs;
mod abs_owner;
mod aggregate_any_value;
pub mod aggregate_any_value_core;
mod aggregate_any_value_owner;
mod aggregate_by;
pub mod aggregate_by_core;
mod aggregate_by_owner;
pub mod aggregate_basic;
pub mod aggregate_distinct_numeric;
mod aggregate_distinct_numeric_kernel;
mod aggregate_distinct_numeric_owner;
mod aggregate_distinct_storage;
mod aggregate_basic_owner;
mod aggregate_count;
mod aggregate_count_owner;
mod aggregate_count_window;
mod aggregate_extrema;
mod aggregate_extrema_dispatch;
mod aggregate_extrema_owner;
mod aggregate_extrema_utf8;
mod aggregate_sum;
mod aggregate_sum_owner;
mod aggregate_window_adapter;
mod bit_shift;
mod bit_shift_owner;
mod bitwise;
mod bitwise_owner;
mod calendar_duration;
mod calendar_epoch_ntz;
mod calendar_convert_tz;
mod calendar_day_number;
mod calendar_day_number_owner;
mod calendar_diff;
mod calendar_diff_owner;
mod calendar_extended;
mod calendar_extended_timestampdiff;
mod calendar_add;
mod calendar_month;
pub mod calendar_add_interval;
pub mod calendar_extended_shared;
mod calendar_extended_format;
mod calendar_extended_parse;
mod calendar_extended_owner;
mod calendar_parts;
mod calendar_parts_owner;
mod calendar_period_diff;
mod calendar_period_diff_owner;
mod collection_cardinality;
mod collection_cardinality_owner;
mod control_owner;
mod crc32;
mod crc32_owner;
mod date;
mod date_owner;
mod dround;
mod dround_owner;
mod makedate;
mod makedate_owner;
mod murmur;
pub mod nullif;
mod nullif_owner;
mod numeric_binary;
mod numeric_binary_owner;
pub mod numeric_elementary;
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
pub(crate) mod round_cast_text;
mod round_owner;
mod rounding_binding;
mod string_append_trailing;
mod string_append_trailing_owner;
mod string_binary;
mod string_case;
mod string_case_owner;
mod string_concat;
mod string_concat_owner;
mod string_concat_ws;
mod string_concat_ws_owner;
pub mod string_extended;
mod string_extended_owner;
mod string_find_in_set;
mod string_find_in_set_owner;
mod string_from_base64;
mod string_from_base64_owner;
mod string_hex;
mod string_hex_owner;
mod string_initcap;
mod string_initcap_owner;
mod string_left_right;
mod string_left_right_owner;
mod string_locate;
mod string_locate_owner;
mod string_md5;
mod string_md5_owner;
mod string_measure;
mod string_measure_owner;
mod string_money;
mod string_pad;
mod string_pad_owner;
mod string_repeat;
mod string_repeat_owner;
mod string_replace;
mod string_replace_owner;
mod string_reverse;
mod string_reverse_owner;
mod string_sha2;
mod string_sha2_owner;
mod string_sm3;
mod string_sm3_owner;
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
mod table_unnest;
mod table_unnest_owner;
#[cfg(test)]
mod table_unnest_tests;
mod truncate;
mod truncate_owner;
mod window_default;
mod window_default_numeric;
mod window_ntile;
mod window_ntile_owner;
mod window_offset;
mod window_offset_owner;
mod window_ranking;
mod window_ranking_owner;

mod window_value;
mod window_value_owner;
#[cfg(test)]
mod window_value_tests;

mod string_regexp_extract;
mod string_regexp_replace;
