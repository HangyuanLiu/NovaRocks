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
use chrono::{Datelike, NaiveDate};

/// Returns whether an Arrow `Date32` value carries NovaRocks's zero-date sentinel.
///
/// The sentinel is a rendering fact shared by every protocol adapter; it is not
/// an execution-kernel capability.
pub fn is_zero_date_sentinel_date32(days: i32) -> bool {
    NaiveDate::from_num_days_from_ce_opt(719163 + days)
        .is_some_and(|date| date.year() == -1 && date.month() == 11 && date.day() == 30)
}
