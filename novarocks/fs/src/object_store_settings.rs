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

use std::borrow::Borrow;
use std::collections::BTreeMap;

use tracing::warn;

use crate::access::ObjectStoreConfig;

/// Runtime retry overrides applied to connector-neutral filesystem resources.
///
/// Operator construction and caching are owned by `novarocks-fs`; Core only
/// projects application configuration onto the neutral resource descriptor.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectStoreRetrySettings {
    pub retry_max_times: Option<usize>,
    pub retry_min_delay_ms: Option<u64>,
    pub retry_max_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub io_timeout_ms: Option<u64>,
}

impl ObjectStoreRetrySettings {
    pub fn from_aws_s3_props<S>(props: Option<&BTreeMap<S, S>>) -> Self
    where
        S: Borrow<str> + Ord,
    {
        let Some(props) = props else {
            return Self::default();
        };
        let mut settings = Self::default();
        if let Some((key, value)) =
            first_nonempty_property(props, &["aws.s3.max_retries", "aws.s3.retry_max_times"])
        {
            settings.retry_max_times = parse_usize_property(&key, value);
        }
        if let Some((key, value)) = first_nonempty_property(props, &["aws.s3.retry_min_delay_ms"]) {
            settings.retry_min_delay_ms = parse_u64_property(&key, value);
        }
        if let Some((key, value)) = first_nonempty_property(props, &["aws.s3.retry_max_delay_ms"]) {
            settings.retry_max_delay_ms = parse_u64_property(&key, value);
        }
        if let Some((key, value)) =
            first_nonempty_property(props, &["aws.s3.request_timeout_ms", "aws.s3.timeout_ms"])
        {
            settings.timeout_ms = parse_u64_property(&key, value);
        }
        if let Some((key, value)) = first_nonempty_property(props, &["aws.s3.io_timeout_ms"]) {
            settings.io_timeout_ms = parse_u64_property(&key, value);
        }
        settings
    }

    /// Fill in any retry knob the caller left unset.
    ///
    /// The values come from the caller's application configuration; this crate
    /// deliberately owns no configuration source of its own.
    pub fn apply_if_absent(&self, cfg: &mut ObjectStoreConfig) {
        if cfg.retry_max_times.is_none() {
            cfg.retry_max_times = self.retry_max_times;
        }
        if cfg.retry_min_delay_ms.is_none() {
            cfg.retry_min_delay_ms = self.retry_min_delay_ms;
        }
        if cfg.retry_max_delay_ms.is_none() {
            cfg.retry_max_delay_ms = self.retry_max_delay_ms;
        }
        if cfg.timeout_ms.is_none() {
            cfg.timeout_ms = self.timeout_ms;
        }
        if cfg.io_timeout_ms.is_none() {
            cfg.io_timeout_ms = self.io_timeout_ms;
        }
    }
}

fn first_nonempty_property<'a, S>(
    props: &'a BTreeMap<S, S>,
    keys: &[&str],
) -> Option<(String, &'a str)>
where
    S: Borrow<str> + Ord,
{
    for key in keys {
        if let Some(value) = props
            .get(*key)
            .map(|v| v.borrow().trim())
            .filter(|v| !v.is_empty())
        {
            return Some(((*key).to_string(), value));
        }
    }
    None
}

fn parse_u64_property(key: &str, value: &str) -> Option<u64> {
    match value.parse::<u64>() {
        Ok(v) => Some(v),
        Err(err) => {
            warn!(
                "ignore invalid object store property {}='{}': {}",
                key, value, err
            );
            None
        }
    }
}

fn parse_usize_property(key: &str, value: &str) -> Option<usize> {
    match value.parse::<usize>() {
        Ok(v) => Some(v),
        Err(err) => {
            warn!(
                "ignore invalid object store property {}='{}': {}",
                key, value, err
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::ObjectStoreRetrySettings;

    #[test]
    fn parse_retry_settings_from_aws_props() {
        let mut props = BTreeMap::new();
        props.insert("aws.s3.max_retries".to_string(), "9".to_string());
        props.insert("aws.s3.retry_min_delay_ms".to_string(), "120".to_string());
        props.insert("aws.s3.retry_max_delay_ms".to_string(), "2800".to_string());
        props.insert("aws.s3.request_timeout_ms".to_string(), "3500".to_string());
        props.insert("aws.s3.io_timeout_ms".to_string(), "4000".to_string());

        let settings = ObjectStoreRetrySettings::from_aws_s3_props(Some(&props));
        assert_eq!(settings.retry_max_times, Some(9));
        assert_eq!(settings.retry_min_delay_ms, Some(120));
        assert_eq!(settings.retry_max_delay_ms, Some(2800));
        assert_eq!(settings.timeout_ms, Some(3500));
        assert_eq!(settings.io_timeout_ms, Some(4000));
    }

    #[test]
    fn parse_retry_settings_ignores_invalid_values() {
        let mut props = BTreeMap::new();
        props.insert("aws.s3.max_retries".to_string(), "x".to_string());
        props.insert("aws.s3.retry_min_delay_ms".to_string(), "abc".to_string());
        let settings = ObjectStoreRetrySettings::from_aws_s3_props(Some(&props));
        assert_eq!(settings.retry_max_times, None);
        assert_eq!(settings.retry_min_delay_ms, None);
    }
}
