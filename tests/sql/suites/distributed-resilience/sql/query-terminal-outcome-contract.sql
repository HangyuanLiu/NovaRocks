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
-- P2 assembly is optional telemetry: the query and P1 proof remain valid,
-- while the retained outcome records typed unavailable evidence.
-- @query_lifecycle_fault=observation-p2-assembly-failure,0
-- @query_control_fragment_backend_limit=3
-- @expect_participant_outcome=proof
-- @expect_lifecycle_telemetry_unavailable=query,runtime_filter_terminal_capture,INJECTED_P2_ASSEMBLY_FAILURE
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.terminal_outcome_contract WHERE sleep(delay_s);

-- query 7
-- The independent P2 budget path has the same non-vetoing contract.
-- @query_lifecycle_fault=observation-p2-budget-pressure,1
-- @query_control_fragment_backend_limit=3
-- @expect_participant_outcome=proof
-- @expect_lifecycle_telemetry_unavailable=query,runtime_filter_terminal_capture,INJECTED_P2_BUDGET_PRESSURE
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.terminal_outcome_contract WHERE sleep(delay_s);

-- query 8
-- Losing the proof control-stream frame must deliver the retained proof via
-- the unary fallback rather than turning a successful participant into an
-- attestation or a timeout.
-- @query_lifecycle_fault=terminal-proof-stream-drop,0
-- @query_control_fragment_backend_limit=3
-- @expect_participant_outcome=proof
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.terminal_outcome_contract WHERE sleep(delay_s);

-- query 9
-- The attestation branch has the same fallback identity and is not dependent
-- on a control-stream frame.
-- @query_lifecycle_fault=terminal-attestation-stream-drop,1
-- @query_lifecycle_fault=terminal-p1-encode-failure,1
-- @query_control_fragment_backend_limit=3
-- @expect_error=CorrectnessEvidenceEncodingFailed
-- @expect_lifecycle_error_source=backend-attestation
-- @expect_participant_outcome=attestation:CorrectnessEvidenceEncodingFailed
SELECT COUNT(*) FROM ${case_db}.terminal_outcome_contract WHERE sleep(delay_s);

-- query 10
-- Once Finalizing has retained immutable terminal evidence, a BE loss is a
-- frontend-owned liveness failure rather than a fabricated backend outcome.
-- @query_lifecycle_fault=terminal-outcome-suppress,1
-- @kill_be_at_lifecycle_phase=1,terminal-retained
-- @query_control_fragment_backend_limit=2
-- @expect_error=query lifecycle terminal ACK failed
-- @expect_lifecycle_error_source=frontend-liveness
-- @expect_lifecycle_metric_delta=heartbeat_timeouts,1
SELECT SUM(left_side.id)
FROM ${case_db}.terminal_outcome_contract left_side
JOIN ${case_db}.terminal_outcome_contract right_side
  ON left_side.id = right_side.id;

-- query 11
-- A faulted lifecycle attempt must not poison the next query.
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.terminal_outcome_contract;

-- query 12
-- @skip_result_check=true
CREATE TABLE ${case_db}.terminal_outcome_p2_write (
  id BIGINT,
  value BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 13
-- P2 must not veto a write or alter the P1 staged-report evidence consumed
-- by the commit path. The Core contract test compares those frame bytes.
-- @query_lifecycle_fault=observation-p2-assembly-failure,0
-- @query_control_fragment_backend_limit=3
-- @expect_participant_outcome=proof
-- @expect_lifecycle_telemetry_unavailable=query,runtime_filter_terminal_capture,INJECTED_P2_ASSEMBLY_FAILURE
-- @skip_result_check=true
INSERT INTO ${case_db}.terminal_outcome_p2_write VALUES (41, 410), (42, 420);

-- query 14
-- @result_contains=2
SELECT COUNT(*) FROM ${case_db}.terminal_outcome_p2_write;
