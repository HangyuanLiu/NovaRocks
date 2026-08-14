-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @sequential=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.terminal_outcome_contract (
  id BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.terminal_outcome_contract VALUES (1, 1), (2, 1), (3, 1);

-- query 3
-- P1 proof encoding must degrade to a retained negative attestation, rather
-- than losing the pre-admitted participant's P0 delivery capability.
-- @query_lifecycle_fault=terminal-p1-encode-failure,0
-- @query_control_fragment_backend_limit=3
-- @expect_error=CorrectnessEvidenceEncodingFailed
-- @expect_lifecycle_error_source=backend-attestation
-- @expect_participant_outcome=attestation:CorrectnessEvidenceEncodingFailed
SELECT COUNT(*) FROM ${case_db}.terminal_outcome_contract WHERE sleep(delay_s);

-- query 4
-- Retention pressure after admission uses the same P0 attestation escape path.
-- @query_lifecycle_fault=terminal-p1-retention-exhausted,1
-- @query_control_fragment_backend_limit=3
-- @expect_error=CorrectnessEvidenceRetentionExhausted
-- @expect_lifecycle_error_source=backend-attestation
-- @expect_participant_outcome=attestation:CorrectnessEvidenceRetentionExhausted
SELECT COUNT(*) FROM ${case_db}.terminal_outcome_contract WHERE sleep(delay_s);

-- query 5
-- A live backend that suppresses all of its terminal delivery paths must
-- converge as NoOutcome, not as an invented liveness failure.
-- @query_lifecycle_fault=terminal-outcome-suppress,2
-- @query_control_fragment_backend_limit=3
-- @expect_error=query lifecycle NoOutcome
-- @expect_lifecycle_error_source=no-outcome
-- @expect_participant_outcome=no-outcome
SELECT COUNT(*) FROM ${case_db}.terminal_outcome_contract WHERE sleep(delay_s);

-- query 6
-- A faulted lifecycle attempt must not poison the next query.
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.terminal_outcome_contract;
