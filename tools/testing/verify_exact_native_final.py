# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
"""Post-run verifier for the original ten exact MySQL native cases.

No CLI, admission-schema guesses, subprocesses, or provider calls. The caller
supplies independently admitted expectations and original launch/wait facts.
The source-pinned runner's successful path is the authority for operation,
typed owner finish, original deadline postchecks, and actual owner joins.
Four ESRCH observations supplement that path; they prove only PID absence.
"""
from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from typing import Callable

JSON_BYTES = 131072
LOG_BYTES = 131072
JSON_DEPTH = 24
JSON_NODES = 16384
STRING_BYTES = 8192
SOURCE_BYTES = 8388608
ROLES = ("fe", "be-0", "be-1", "be-2")
CASES = frozenset(
    [f"exact-native-resident-cut-{n}" for n in (1, 2, 3, 4, 5, 6, 1048575, 1048576, 1048577)]
    + ["exact-native-missing-tail-cut-1048577"]
)
RECEIPT_SETTLEMENT = "requires successful original deadline postcheck after write and sync"
ROLE_SETTLEMENT = "requires runner's later settled successful schema5 evidence"
TIMING_ORIGIN = "scene operation entry; all deadlines use original prelaunch absolute Instant"
SCOPE = ("original framing/source facts + wire + context-held/public owner convergence; "
         "excludes allocator last alias, fullClosing64, lateACKalias")
PROVENANCE_FIELDS = frozenset((
    "clean_revision", "source_tree_sha256", "server_binary_sha256", "server_build_identity",
    "runner_binary_sha256", "base_config_sha256", "frozen_execution_binding_sha256",
    "large_input_sha256", "tiny_input_sha256",
))
SCHEMA5_FIELDS = frozenset((
    "schema_version", "scenario", "outcome", "exit_code", "started_unix_millis",
    "ended_unix_millis", "command", "source_revision", "source_dirty", "source_tree_sha256",
    "runner_native_build_identity", "runner_executable", "cargo_lock_sha256", "platform",
    "actions", "phase_observations", "runtime_dir", "primary_binary", "base_config_path",
    "base_config_sha256", "cluster_size", "launch_profile", "process_launch_identities",
    "effective_launch_config_sha256", "effective_launch_config_semantics_sha256",
    "effective_launch_config", "diagnostics",
))
OBSERVATION_FIELDS = frozenset((
    "schema_version", "case", "native_acceptance", "operation_before_receipt_pass",
    "receipt_settlement", "all_four_role_exit", "admitted_provenance",
    "original_launch_identities", "exact_sql_sha256", "cut_row_wire_bytes",
    "expected_row_sha256", "expected_prefix_sha256", "controls", "control_prefixes",
    "final_control_prefixes", "held_roots", "independent_root", "raw_original_binding",
    "reader", "kill_started_us", "kill_returned_us", "resumed_us", "timing_origin",
    "recovery_before_tasks_created", "recovery_after_tasks_created", "errors", "scope",
))
# These four fields exist in the current source; they are mandatory, not a shortcut.
EFFECTIVE_OBSERVATION_FIELDS = frozenset((
    "effective_launch_config_sha256", "effective_launch_config_semantics_sha256",
    "effective_launch_config_bytes", "original_prelaunch_config_artifact",
))


class VerificationFailure(Exception):
    """Finite presentation. Input payloads, paths, and source errors are never rendered."""
    def __init__(self, code: str):
        if not re.fullmatch(r"[a-z_]{1,64}", code):
            code = "internal_failure"
        self.code = code
        super().__init__(code)

    def __str__(self) -> str:
        return "exact_native_verification_refused code=" + self.code

    __repr__ = __str__


def require(condition: bool, code: str) -> None:
    if not condition:
        raise VerificationFailure(code) from None


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _hex(value: object, length: int = 64) -> bool:
    return type(value) is str and re.fullmatch(r"[0-9a-f]{" + str(length) + r"}", value) is not None


def _uint(value: object, maximum: int = (1 << 64) - 1) -> bool:
    return type(value) is int and 0 <= value <= maximum


def _text(value: object, maximum: int = STRING_BYTES) -> bool:
    if type(value) is not str or len(value) > maximum:
        return False
    try:
        return len(value.encode("utf-8")) <= maximum
    except UnicodeError:
        return False


def _path(value: object) -> bool:
    return (_text(value, 4096) and value.startswith("/")
            and all(ord(ch) >= 32 and ord(ch) != 127 for ch in value))


def _build_id(value: object) -> bool:
    return (type(value) is str and value != "unknown"
            and re.fullmatch(r"[A-Za-z0-9._-]{1,128}", value) is not None)


def _json_equal(left: object, right: object) -> bool:
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        return set(left) == set(right) and all(_json_equal(left[key], right[key]) for key in left)
    if type(left) is list:
        return len(left) == len(right) and all(_json_equal(a, b) for a, b in zip(left, right))
    return left == right


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, "json_duplicate_key")
        result[key] = value
    return result


def _no_constant(_: str) -> None:
    raise VerificationFailure("json_nonfinite") from None


def parse_finite_json(raw: bytes) -> dict:
    require(type(raw) is bytes and 0 < len(raw) <= JSON_BYTES, "json_bytes_bound")
    # Check nesting before json.loads can recurse. Braces inside strings do not count.
    depth = 0
    quoted = False
    escaped = False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            require(depth <= JSON_DEPTH, "json_depth_bound")
        elif byte in (93, 125):
            depth -= 1
            require(depth >= 0, "json_malformed")
    require(not quoted and depth == 0, "json_malformed")
    try:
        value = json.loads(raw.decode("utf-8", "strict"), object_pairs_hook=_unique_object,
                           parse_constant=_no_constant)
    except VerificationFailure:
        raise
    except (UnicodeError, ValueError, RecursionError, OverflowError):
        raise VerificationFailure("json_malformed") from None
    count = 0
    stack = [(value, 0)]
    while stack:
        item, level = stack.pop()
        count += 1
        require(count <= JSON_NODES and level <= JSON_DEPTH, "json_structure_bound")
        if type(item) is dict:
            require(len(item) <= 512, "json_object_bound")
            for key, child in item.items():
                require(_text(key, 256), "json_key_bound")
                stack.append((child, level + 1))
        elif type(item) is list:
            require(len(item) <= 512, "json_array_bound")
            stack.extend((child, level + 1) for child in item)
        elif type(item) is str:
            try:
                require(_text(item), "json_string_bound")
            except UnicodeError:
                raise VerificationFailure("json_malformed") from None
        else:
            require(item is None or type(item) is bool
                    or (type(item) is int and abs(item) <= (1 << 128) - 1)
                    or (type(item) is float and math.isfinite(item)),
                    "json_scalar_kind")
    require(type(value) is dict, "json_object_required")
    return value


@dataclass(frozen=True, slots=True, repr=False)
class LaunchIdentity:
    role: str
    pid: int
    process_start_token: str

    def __repr__(self) -> str:
        return "LaunchIdentity(finite identity omitted)"

    def validate(self) -> None:
        require(self.role in ROLES and type(self.role) is str, "role_identity")
        require(type(self.pid) is int and 0 < self.pid <= 2147483647, "positive_original_pid")
        require(type(self.process_start_token) is str
                and re.fullmatch(r"[A-Za-z0-9:._-]{1,256}", self.process_start_token) is not None,
                "start_token")

    def json(self) -> dict:
        return {"role": self.role, "pid": self.pid, "process_start_token": self.process_start_token}


def parse_roles(value: object) -> tuple[LaunchIdentity, ...]:
    require(type(value) is list and len(value) == 4, "four_original_roles")
    result = []
    for item in value:
        require(type(item) is dict and set(item) == {"role", "pid", "process_start_token"},
                "role_identity_fields")
        identity = LaunchIdentity(item["role"], item["pid"], item["process_start_token"])
        identity.validate()
        result.append(identity)
    roles = tuple(result)
    require(tuple(item.role for item in roles) == ROLES, "original_role_order")
    require(len({item.pid for item in roles}) == 4, "original_pid_duplicate")
    return roles


@dataclass(frozen=True, slots=True, repr=False)
class SourceSnapshot:
    """Actual git outputs collected independently from the observation/receipt.

    revision/status are the same trimmed UTF-8 strings used by schema5.
    The other three fields are unmodified git output bytes, including newlines.
    """
    revision: str
    status: str
    tracked: bytes
    diff: bytes
    staged: bytes

    def __repr__(self) -> str:
        return "SourceSnapshot(raw source outputs omitted)"

    def clean_tree_sha256(self) -> str:
        require(_hex(self.revision, 40), "source_full_revision")
        require(type(self.status) is str and self.status == "", "source_dirty")
        require(all(type(part) is bytes and len(part) <= SOURCE_BYTES
                    for part in (self.tracked, self.diff, self.staged)), "source_output_bound")
        require(self.tracked != b"" and self.diff == b"" and self.staged == b"", "source_dirty")
        digest = hashlib.sha256()
        for part in (self.revision.encode("utf-8"), self.status.encode("utf-8"),
                     self.tracked, self.diff, self.staged):
            digest.update(part)
            digest.update(b"\0")
        return digest.hexdigest()


@dataclass(frozen=True, slots=True, repr=False)
class AdmittedExpected:
    """Caller-admitted inputs. Never fill any member from the receipt under test.

    roles/effective hashes are separately bound to the original launch owner;
    they cannot be known from a prelaunch static template alone.
    """
    case: str
    provenance: dict
    server_build_commit: str
    runner_build_commit: str
    runner_build_identity: str
    cargo_lock_sha256: str
    effective_launch_config_sha256: str
    roles: tuple[LaunchIdentity, ...]
    server_path: str
    runner_path: str
    base_config_path: str
    runtime_dir: str
    evidence_path: str
    sql_sha256: str
    row_sha256: str
    prefix_sha256: str
    cut_row_wire_bytes: int

    def __repr__(self) -> str:
        return "AdmittedExpected(independent admitted inputs omitted)"


@dataclass(frozen=True, slots=True, repr=False)
class ActualMaterials:
    """Separately measured actual source, files/builds, original wait and log.

    Binary/lock digests must come from the explicit original paths, not JSON.
    Build facts must be obtained from the actual binary/build provenance.
    runner_settled/returncode are the original runner wait result, never a
    scenario JSON flag or a directory-empty inference. No formatter is called.
    """
    source: SourceSnapshot
    server_binary_sha256: str
    runner_binary_sha256: str
    server_build_commit: str
    server_build_identity: str
    server_exact_mysql_write: bool
    runner_build_commit: str
    runner_build_identity: str
    cargo_lock_sha256: str
    base_config_bytes: bytes
    effective_launch_config_bytes: bytes
    runner_settled: bool
    runner_returncode: int | None
    runner_log: bytes
    schema5_bytes: bytes
    observation_bytes: bytes

    def __repr__(self) -> str:
        return "ActualMaterials(actual raw materials omitted)"


def parse_server_build_identity(raw: bytes) -> tuple[str, str]:
    """Parse the actual feature-only binary's existing bounded stdout marker.

    The caller owns invocation, original exit-status verification, and finite
    capture. This parser performs no executable launch.
    """
    require(type(raw) is bytes and 0 < len(raw) <= 384, "server_build_marker_bound")
    match = re.fullmatch(
        rb"NOVAROCKS_MEM_1_M07_BUILD commit=([0-9a-f]{40}) "
        rb"build_identity=([A-Za-z0-9._-]{1,128}) exact_mysql_write=true\n", raw)
    require(match is not None, "server_build_marker")
    commit, identity = (part.decode("ascii") for part in match.groups())
    require(_build_id(identity), "server_build_marker")
    return commit, identity


def _validate_expected(expected: AdmittedExpected) -> None:
    require(type(expected) is AdmittedExpected and type(expected.case) is str and expected.case in CASES, "admitted_case")
    p = expected.provenance
    require(type(p) is dict and set(p) == PROVENANCE_FIELDS, "admitted_provenance_fields")
    require(_hex(p["clean_revision"], 40), "admitted_full_revision")
    require(_build_id(p["server_build_identity"]), "admitted_build_identity")
    require(all(_hex(p[key]) for key in PROVENANCE_FIELDS
                - {"clean_revision", "server_build_identity"}), "admitted_hash")
    require(expected.server_build_commit == p["clean_revision"]
            and expected.runner_build_commit == p["clean_revision"], "admitted_build_commit")
    require(_build_id(expected.runner_build_identity), "admitted_build_identity")
    require(all(_hex(value) for value in (expected.cargo_lock_sha256,
                expected.effective_launch_config_sha256, expected.sql_sha256,
                expected.row_sha256, expected.prefix_sha256)), "admitted_hash")
    require(type(expected.roles) is tuple and len(expected.roles) == 4, "four_original_roles")
    require(parse_roles([item.json() for item in expected.roles]) == expected.roles, "admitted_roles")
    require(all(_path(value) for value in (expected.server_path, expected.runner_path,
                expected.base_config_path, expected.runtime_dir, expected.evidence_path)), "admitted_path")
    require(_uint(expected.cut_row_wire_bytes) and expected.cut_row_wire_bytes > 0,
            "admitted_cut")
    require(expected.case.endswith("-" + str(expected.cut_row_wire_bytes)), "admitted_cut")


def _validate_actual(expected: AdmittedExpected, actual: ActualMaterials) -> dict:
    require(type(actual) is ActualMaterials, "actual_materials_type")
    # Do not touch original role PIDs before the actual runner has settled.
    require(actual.runner_settled is True, "runner_unsettled")
    require(type(actual.runner_returncode) is int, "runner_exit_unknown")
    require(actual.runner_returncode == 0, "runner_exit_failed")
    p = expected.provenance
    require(type(actual.source) is SourceSnapshot, "actual_source_type")
    require(actual.source.revision == p["clean_revision"]
            and actual.source.clean_tree_sha256() == p["source_tree_sha256"], "actual_source_mismatch")
    for actual_hash, expected_hash in (
        (actual.server_binary_sha256, p["server_binary_sha256"]),
        (actual.runner_binary_sha256, p["runner_binary_sha256"]),
        (actual.cargo_lock_sha256, expected.cargo_lock_sha256),
    ):
        require(_hex(actual_hash) and actual_hash == expected_hash, "actual_file_hash_mismatch")
    require(actual.server_build_commit == expected.server_build_commit
            and actual.runner_build_commit == expected.runner_build_commit, "actual_build_commit_mismatch")
    require(actual.server_build_identity == p["server_build_identity"]
            and actual.runner_build_identity == expected.runner_build_identity,
            "actual_build_identity_mismatch")
    require(actual.server_exact_mysql_write is True, "actual_server_feature")
    require(type(actual.base_config_bytes) is bytes
            and 0 < len(actual.base_config_bytes) <= JSON_BYTES, "base_config_bytes_bound")
    require(_digest(actual.base_config_bytes) == p["base_config_sha256"], "actual_base_config_mismatch")
    effective = parse_finite_json(actual.effective_launch_config_bytes)
    require(set(effective) == {"schema_version", "semantics"}
            and type(effective["schema_version"]) is int and effective["schema_version"] == 1
            and type(effective["semantics"]) is dict, "effective_schema")
    require(_digest(actual.effective_launch_config_bytes) == expected.effective_launch_config_sha256,
            "actual_effective_config_mismatch")
    return effective


def _validate_runner_log(expected: AdmittedExpected, raw: bytes) -> None:
    require(type(raw) is bytes and 0 < len(raw) <= LOG_BYTES, "runner_log_bound")
    try:
        lines = raw.decode("utf-8", "strict").splitlines()
    except UnicodeError:
        raise VerificationFailure("runner_log_encoding") from None
    require(len(lines) <= 512 and all(len(line.encode("utf-8")) <= STRING_BYTES for line in lines),
            "runner_log_structure_bound")
    passes = [line for line in lines if line.startswith("scenario=")]
    require(passes == [f"scenario={expected.case} PASS evidence={expected.evidence_path}"],
            "runner_case_pass_line")
    require(not any(line.startswith("system scenario runner failed:") for line in lines),
            "runner_failure_line")


def _validate_schema5(expected: AdmittedExpected, actual: ActualMaterials, effective: dict) -> dict:
    evidence = parse_finite_json(actual.schema5_bytes)
    require(set(evidence) == SCHEMA5_FIELDS, "scenario_fields")
    require(type(evidence["schema_version"]) is int and evidence["schema_version"] == 5,
            "scenario_schema")
    require(evidence["scenario"] == expected.case and evidence["outcome"] == "passed"
            and type(evidence["exit_code"]) is int and evidence["exit_code"] == 0,
            "scenario_not_passed")
    require(evidence["diagnostics"] is None and evidence["source_dirty"] is False,
            "scenario_diagnostics_or_dirty")
    p = expected.provenance
    for name, value in {
        "source_revision": p["clean_revision"], "source_tree_sha256": p["source_tree_sha256"],
        "runner_native_build_identity": expected.runner_build_identity,
        "runner_executable": expected.runner_path, "cargo_lock_sha256": expected.cargo_lock_sha256,
        "runtime_dir": expected.runtime_dir, "primary_binary": expected.server_path,
        "base_config_path": expected.base_config_path, "base_config_sha256": p["base_config_sha256"],
        "effective_launch_config_sha256": expected.effective_launch_config_sha256,
        "effective_launch_config_semantics_sha256": expected.effective_launch_config_sha256,
        "launch_profile": "fault-scenario",
    }.items():
        require(evidence[name] == value, "scenario_binding_mismatch")
    require(type(evidence["cluster_size"]) is int and evidence["cluster_size"] == 3,
            "scenario_cluster_size")
    require(_json_equal(evidence["effective_launch_config"], effective), "scenario_effective_value_mismatch")
    require(parse_roles(evidence["process_launch_identities"]) == expected.roles,
            "scenario_original_roles_mismatch")
    require(_uint(evidence["started_unix_millis"], (1 << 128) - 1)
            and _uint(evidence["ended_unix_millis"], (1 << 128) - 1)
            and evidence["ended_unix_millis"] >= evidence["started_unix_millis"], "scenario_time_fields")
    actions = evidence["actions"]
    require(type(actions) is list and all(_text(item) for item in actions), "scenario_actions")
    require(actions.count("scenario assertions passed") == 1
            and actions.count("cluster and fixture cleanup passed") == 1
            and actions.index("scenario assertions passed") < actions.index("cluster and fixture cleanup passed"),
            "scenario_cleanup_not_passed")
    require(type(evidence["command"]) is list and 0 < len(evidence["command"]) <= 128
            and all(_text(item) for item in evidence["command"])
            and evidence["command"][0] == expected.runner_path, "scenario_command")
    require(type(evidence["phase_observations"]) is list, "scenario_phase_observations")
    platform = evidence["platform"]
    require(type(platform) is dict and set(platform) == {"os", "architecture", "logical_cpu_count"}
            and _text(platform["os"], 64) and _text(platform["architecture"], 64)
            and _uint(platform["logical_cpu_count"], 65536) and platform["logical_cpu_count"] > 0,
            "scenario_platform")
    return evidence


def _validate_observation(expected: AdmittedExpected, actual: ActualMaterials) -> dict:
    observation = parse_finite_json(actual.observation_bytes)
    keys = set(observation)
    require(keys == OBSERVATION_FIELDS | EFFECTIVE_OBSERVATION_FIELDS,
            "observation_fields")
    require(type(observation["schema_version"]) is int and observation["schema_version"] == 1,
            "observation_schema")
    require(observation["case"] == expected.case and observation["native_acceptance"] is False
            and observation["operation_before_receipt_pass"] is True, "precleanup_operation_not_passed")
    require(observation["receipt_settlement"] == RECEIPT_SETTLEMENT
            and observation["all_four_role_exit"] == ROLE_SETTLEMENT
            and observation["timing_origin"] == TIMING_ORIGIN and observation["scope"] == SCOPE,
            "observation_scope")
    require(type(observation["errors"]) is list and len(observation["errors"]) == 5
            and all(item is None for item in observation["errors"]), "observation_failed_or_unknown")
    require(type(observation["admitted_provenance"]) is dict
            and observation["admitted_provenance"] == expected.provenance, "observation_provenance_mismatch")
    roles = observation["original_launch_identities"]
    require(type(roles) is dict and set(roles) == {"frontend", "backends"}
            and type(roles["backends"]) is list and len(roles["backends"]) == 3,
            "observation_original_roles")
    require(parse_roles([roles["frontend"]] + roles["backends"]) == expected.roles,
            "observation_original_roles_mismatch")
    for key, value in {"exact_sql_sha256": expected.sql_sha256,
                       "expected_row_sha256": expected.row_sha256,
                       "expected_prefix_sha256": expected.prefix_sha256,
                       "cut_row_wire_bytes": expected.cut_row_wire_bytes}.items():
        require(observation[key] == value and type(observation[key]) is type(value),
                "observation_case_input_mismatch")
    if EFFECTIVE_OBSERVATION_FIELDS <= keys:
        require(observation["effective_launch_config_sha256"] == expected.effective_launch_config_sha256
                and observation["effective_launch_config_semantics_sha256"] == expected.effective_launch_config_sha256
                and type(observation["effective_launch_config_bytes"]) is int
                and observation["effective_launch_config_bytes"] == len(actual.effective_launch_config_bytes)
                and observation["original_prelaunch_config_artifact"] == "exact-native-effective-launch-config.json",
                "observation_effective_mismatch")
    # Detailed control/Root/wire predicates are source-pinned runner assertions.
    # Require their successful evidence to exist, without pretending a scalar
    # DTO or Debug string independently proves physical owners or ACK sources.
    require(type(observation["controls"]) is list and len(observation["controls"]) == 16
            and type(observation["controls"][0]) is str,
            "observation_controls_missing")
    controls = observation["controls"]
    prefixes = observation["control_prefixes"]
    require(type(prefixes) is list and len(prefixes) == 16
            and all(value is None or (_text(value) and value != "") for value in controls + prefixes)
            and all((a is None) == (b is None) for a, b in zip(controls, prefixes)),
            "observation_control_structure")
    count = next((index for index, value in enumerate(controls) if value is None), 16)
    require(all(value is None for value in controls[count:]), "observation_control_structure")
    require(all(_text(observation[name]) and observation[name] != "" for name in
                ("independent_root", "raw_original_binding", "reader", "final_control_prefixes")),
            "observation_facts_missing")
    held = observation["held_roots"]
    require(type(held) is list and len(held) == 3
            and all(type(item) is dict and all(_uint(value) for value in item.values()) for item in held),
            "observation_held_roots_structure")
    require(all(type(observation[name]) is list and len(observation[name]) == 3
                and all(_uint(value) for value in observation[name]) for name in
                ("recovery_before_tasks_created", "recovery_after_tasks_created")),
            "observation_recovery_structure")
    for key in ("kill_started_us", "kill_returned_us", "resumed_us"):
        require(_uint(observation[key]), "observation_timing_missing")
    require(observation["kill_started_us"] <= observation["kill_returned_us"] <= observation["resumed_us"],
            "observation_timing_order")
    return observation


def verify_final(expected: AdmittedExpected, actual: ActualMaterials, *,
                 kill: Callable[[int, int], object] = os.kill) -> dict:
    """Verify after the original runner was waited/reaped; inspect exactly four PIDs.

    `kill` injection exists only for pure tests. Production integration must
    use os.kill directly. No PG probes, descendant scans, directory-empty
    shortcuts, retry loops, or formatter calls are performed.
    """
    _validate_expected(expected)
    effective = _validate_actual(expected, actual)
    _validate_runner_log(expected, actual.runner_log)
    _validate_schema5(expected, actual, effective)
    _validate_observation(expected, actual)
    absent = []
    for identity in expected.roles:
        try:
            kill(identity.pid, 0)
        except OSError as error:
            require(type(error.errno) is int and error.errno == errno.ESRCH, "original_pid_absence_unknown")
            absent.append({**identity.json(), "probe": "kill(pid,0)", "result": "ESRCH"})
        except BaseException:
            raise VerificationFailure("original_pid_absence_unknown") from None
        else:
            raise VerificationFailure("original_pid_present") from None
    return {
        "schema_version": 1, "case": expected.case, "native_acceptance": True,
        "runner_returncode": 0, "scenario_outcome": "passed", "scenario_schema_version": 5,
        "source_revision": expected.provenance["clean_revision"],
        "source_tree_sha256": expected.provenance["source_tree_sha256"], "source_dirty": False,
        "server_binary_sha256": actual.server_binary_sha256,
        "runner_binary_sha256": actual.runner_binary_sha256,
        "base_config_sha256": _digest(actual.base_config_bytes),
        "effective_launch_config_sha256": _digest(actual.effective_launch_config_bytes),
        "effective_launch_config_semantics_sha256": _digest(actual.effective_launch_config_bytes),
        "effective_launch_config_bytes": len(actual.effective_launch_config_bytes),
        "scenario_evidence_sha256": _digest(actual.schema5_bytes),
        "operation_observation_sha256": _digest(actual.observation_bytes),
        "original_runner_log_sha256": _digest(actual.runner_log),
        "original_role_absence_after_runner_settled": absent,
        "scope": "original exact-case runner acceptance plus four original PID ESRCH observations; excludes process groups, descendants, allocator backing/last alias, fullClosing64, lateACKalias",
    }
