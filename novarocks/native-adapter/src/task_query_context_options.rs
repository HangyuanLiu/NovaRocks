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

//! Immutable Native query-options facts shared by the role-local task hosts.

use std::sync::Arc;

use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_execution_contract::task_execution::domain::{CodecOwnedContent, ContentFingerprint};
use novarocks_task_codec::domain::WireContent;
use novarocks_task_codec::operation::ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG;

#[derive(Clone, Debug)]
pub struct QueryContextOptions {
    runtime: Arc<QueryOptions>,
    fingerprint: ContentFingerprint,
    query_wide_fingerprint: ContentFingerprint,
}

pub fn query_options_fingerprint(
    wire: novarocks_proto_models::novarocks::QueryOptions,
) -> ContentFingerprint {
    WireContent::new(ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG, wire).fingerprint()
}

/// Compares every original wire field except the task-local pipeline degree.
pub fn query_wide_options_fingerprint(
    mut wire: novarocks_proto_models::novarocks::QueryOptions,
) -> ContentFingerprint {
    wire.pipeline_dop = 0;
    query_options_fingerprint(wire)
}

impl QueryContextOptions {
    pub const fn new(
        runtime: Arc<QueryOptions>,
        fingerprint: ContentFingerprint,
        query_wide_fingerprint: ContentFingerprint,
    ) -> Self {
        Self {
            runtime,
            fingerprint,
            query_wide_fingerprint,
        }
    }

    pub fn runtime(&self) -> &Arc<QueryOptions> {
        &self.runtime
    }

    pub const fn fingerprint(&self) -> ContentFingerprint {
        self.fingerprint
    }

    pub const fn query_wide_fingerprint(&self) -> ContentFingerprint {
        self.query_wide_fingerprint
    }
}

#[cfg(test)]
mod tests {
    use novarocks_proto_models::novarocks::QueryOptions;

    use super::{query_options_fingerprint, query_wide_options_fingerprint};

    #[test]
    fn only_pipeline_dop_is_excluded_from_the_query_wide_wire_witness() {
        let original = QueryOptions {
            pipeline_dop: 8,
            query_timeout: -1,
            runtime_filter_wait_timeout_ms: Some(0),
            ..Default::default()
        };
        let mut task = original;
        task.pipeline_dop = 1;
        assert_ne!(
            query_options_fingerprint(original),
            query_options_fingerprint(task)
        );
        assert_eq!(
            query_wide_options_fingerprint(original),
            query_wide_options_fingerprint(task)
        );

        task.query_timeout = 0;
        assert_ne!(
            query_wide_options_fingerprint(original),
            query_wide_options_fingerprint(task),
            "the wire's -1 and 0 must not collapse through runtime projection"
        );
        task.query_timeout = -1;
        task.runtime_filter_wait_timeout_ms = None;
        assert_ne!(
            query_wide_options_fingerprint(original),
            query_wide_options_fingerprint(task),
            "an explicitly present zero must remain distinct from absence"
        );
    }
}
