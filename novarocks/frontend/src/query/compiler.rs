// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Frontend query-admission compiler boundary.
//!
//! This type is the only compiler surface consumed by the SQL session router.
//! It preserves the Core kernel's sealed request and completion contracts while
//! keeping Frontend admission independent from the Core `engine` namespace.

use novarocks::query_execution::PreparedQueryOperation;
use novarocks::query_execution::compiler::CoreQueryCompiler;
use novarocks::query_execution::request_context::RequestContext;
use novarocks_execution::runtime::query_options::QueryOptions;

#[derive(Clone)]
pub(crate) struct FrontendQueryCompiler {
    kernel: CoreQueryCompiler,
}

impl FrontendQueryCompiler {
    pub(crate) fn new(kernel: CoreQueryCompiler) -> Self {
        Self { kernel }
    }

    pub(crate) fn prepare(
        &self,
        sql: &str,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
    ) -> Result<PreparedQueryOperation, String> {
        self.kernel.prepare(sql, context, query_options)
    }
}
