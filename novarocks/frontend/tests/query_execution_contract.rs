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

use novarocks::query_execution::contract::{
    DistributedQueryCoordinator, DistributedQueryError, DistributedQueryIntent,
    DistributedQueryOutcome, DistributedQueryRequest,
};
use novarocks::runtime::query_result::QueryResult;

struct FrontendSuccessCoordinator;

impl DistributedQueryCoordinator for FrontendSuccessCoordinator {
    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        Ok(request.into_success(QueryResult {
            columns: Vec::new(),
            chunks: Vec::new(),
        }))
    }
}

#[test]
fn frontend_coordinator_consumes_opaque_request_and_returns_result_outcome() {
    let coordinator: &dyn DistributedQueryCoordinator = &FrontendSuccessCoordinator;
    let outcome = coordinator
        .execute(DistributedQueryRequest::for_contract_test(
            DistributedQueryIntent::Result,
        ))
        .expect("frontend coordinator must return a successful outcome");

    assert_eq!(outcome.intent(), DistributedQueryIntent::Result);
    assert_eq!(outcome.into_query_result().row_count(), 0);
}
