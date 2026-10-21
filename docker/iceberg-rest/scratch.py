#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run-scoped fixture identities, cumulative budgets and conservative collection.

No liveness registry or worktree lease is involved. Callers own the machine
lock and the actual writer processes; these helpers grant neither authority.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import http.client
import json
import math
from pathlib import Path
import re
import secrets
import shlex
import socket
import time
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

import fixture_runtime as runtime

RUN_RE = re.compile(r"nr_scratch_([0-9]{13})_([0-9a-f]{32})\Z")
RESOURCE_RE = re.compile(r"(nr_scratch_[0-9]{13}_[0-9a-f]{32})(?:_([a-z][a-z0-9]{0,11}))?\Z")
IAM_RE = re.compile(r"nr([0-9a-f]{8})([0-9a-f]{10})\Z")
PURPOSE_RE = re.compile(r"[a-z][a-z0-9]{0,11}\Z")
MISSING_CODES = {"NoSuchKey", "NoSuchBucket", "NoSuchUser", "NoSuchPolicy",
                 "XMinioAdminNoSuchUser", "XMinioAdminNoSuchPolicy"}
MAX_REST_BYTES = 8 * 1024 * 1024
MAX_MC_BYTES = 16 * 1024 * 1024
MAX_PREFIX_VISITS = 2048
MAX_PREFIX_DEPTH = 32
MAX_PENDING_PREFIXES = 10000


class _DeadlineSocket(socket.socket):
    """SocketIO calls recv_into for every read, including header parsing.

    Reapplying the original deadline prevents continuous slow progress from
    resetting the budget. The connection owner closes this exact socket.
    """
    def __init__(self, original, deadline, monotonic):
        super().__init__(original.family, original.type, original.proto, fileno=original.detach())
        self.deadline, self.monotonic = deadline, monotonic

    def recv_into(self, *args, **kwargs):
        self._remaining_timeout()
        return super().recv_into(*args, **kwargs)

    def sendall(self, *args, **kwargs):
        self._remaining_timeout()
        return super().sendall(*args, **kwargs)

    def _remaining_timeout(self):
        remaining = self.deadline - self.monotonic()
        if remaining <= 0:
            raise TimeoutError("original REST deadline expired")
        self.settimeout(remaining)


class _DeadlineConnection(http.client.HTTPConnection):
    def __init__(self, port, deadline, monotonic):
        self.deadline, self.monotonic = deadline, monotonic
        super().__init__("127.0.0.1", port, timeout=max(0.001, deadline - monotonic()))

    def connect(self):
        super().connect()
        self.sock = _DeadlineSocket(self.sock, self.deadline, self.monotonic)


def s3_parts(uri: str) -> tuple[str, str]:
    if not isinstance(uri, str):
        raise runtime.RuntimeFailure("InvalidScratchScope", "S3 scope must be a string")
    parsed = urlsplit(uri)
    parts = parsed.path.strip("/").split("/") if parsed.path.strip("/") else []
    if (parsed.scheme != "s3" or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", parsed.netloc)
            or parsed.query or parsed.fragment or "%" in uri or "//" in parsed.path
            or any(p in {"", ".", ".."} or any(ord(c) <= 32 for c in p) for p in parts)):
        raise runtime.RuntimeFailure("InvalidScratchScope", "invalid S3 scope")
    return parsed.netloc, "/".join(parts)


def canonical_s3(uri: str) -> str:
    bucket, key = s3_parts(uri)
    return "s3://" + bucket + ("/" + key if key else "")


def contains(root: str, value: str) -> bool:
    root, value = canonical_s3(root), canonical_s3(value)
    return value == root or value.startswith(root + "/")


def resource_run(name: str) -> str | None:
    match = RESOURCE_RE.fullmatch(name) if isinstance(name, str) and len(name) <= 63 else None
    return match[1] if match else None


def created_ms(run_id: str) -> int:
    match = RUN_RE.fullmatch(run_id) if isinstance(run_id, str) else None
    if not match:
        raise runtime.RuntimeFailure("InvalidScratchIdentity", "invalid scratch run identity")
    return int(match[1])


def parse_iam_name(name: str) -> int | None:
    """Return the embedded creation time; this is a reserved scratch grammar."""
    match = IAM_RE.fullmatch(name) if isinstance(name, str) else None
    return int(match[1], 16) * 1000 if match else None


@dataclass(frozen=True)
class ScratchIdentity:
    run_id: str
    scope_root: str

    def __post_init__(self):
        created_ms(self.run_id)
        root = canonical_s3(self.scope_root)
        if not root.endswith("/scratch/" + self.run_id):
            raise runtime.RuntimeFailure("InvalidScratchScope", "scope must end in scratch/run_id")
        object.__setattr__(self, "scope_root", root)

    @classmethod
    def create(cls, worktree_root: str, *, now_ms: int | None = None,
               token: str | None = None) -> ScratchIdentity:
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        if type(now_ms) is not int or not 10**12 <= now_ms < 10**13:
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "scratch creation time must have 13 digits")
        token = secrets.token_hex(16) if token is None else token
        run_id = f"nr_scratch_{now_ms:013d}_{token}"
        return cls(run_id, canonical_s3(worktree_root) + "/scratch/" + run_id)

    @classmethod
    def from_dict(cls, value: dict) -> ScratchIdentity:
        if not isinstance(value, dict) or value.get("schema") != 1:
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "invalid scratch identity schema")
        result = cls(value.get("run_id"), value.get("scope_root"))
        if value.get("created_unix_ms") != created_ms(result.run_id):
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "scratch timestamp contradicts its name")
        return result

    def resource_name(self, purpose: str | None = None) -> str:
        if purpose is None:
            return self.run_id
        if not PURPOSE_RE.fullmatch(purpose):
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "invalid scratch purpose")
        name = self.run_id + "_" + purpose
        if len(name) > 63:
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "scratch resource name exceeds 63 characters")
        return name

    def iam_name(self, purpose: str) -> str:
        if not PURPOSE_RE.fullmatch(purpose):
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "invalid scratch purpose")
        epoch = created_ms(self.run_id) // 1000
        if epoch > 0xffffffff:
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "IAM timestamp is out of range")
        suffix = hashlib.sha256((self.run_id + ":" + purpose).encode()).hexdigest()[:10]
        return f"nr{epoch:08x}{suffix}"

    def as_dict(self) -> dict:
        return dict(schema=1, run_id=self.run_id, created_unix_ms=created_ms(self.run_id),
                    scope_root=self.scope_root)


@dataclass(frozen=True)
class RunBudget:
    identity: ScratchIdentity
    started_unix_ms: int
    deadline_unix_ms: int
    cleanup_grace_seconds: int
    deadline: float
    monotonic: Any = time.monotonic

    @classmethod
    def admit(cls, record: dict, identity: ScratchIdentity, *, seconds: int = 3600,
              inherited: dict | None = None, wall_clock=time.time,
              monotonic=time.monotonic, cleanup: bool = False) -> RunBudget:
        if type(cleanup) is not bool or (cleanup and inherited is None):
            raise runtime.RuntimeFailure("RunBudgetRejected", "cleanup requires an inherited original budget")
        policy = record.get("policy", record.get("config", {}).get("policy", {}))
        maximum, grace, ttl = (policy.get(k) for k in
                              ("max_run_seconds", "cleanup_grace_seconds", "scratch_ttl_seconds"))
        if (any(type(n) is not int or n <= 0 for n in (maximum, grace, ttl))
                or ttl <= 2 * (maximum + grace) or maximum <= grace
                or (inherited is None and (type(seconds) is not int or seconds <= grace or seconds > maximum))):
            raise runtime.RuntimeFailure("RunBudgetRejected", "invalid or excessive complete fixture budget")
        now = int(wall_clock() * 1000)
        if created_ms(identity.run_id) > now + 5000:
            raise runtime.RuntimeFailure("RunBudgetRejected", "scratch creation time is in the future")
        start, end = now, now
        if inherited is None:
            end += seconds * 1000
        if inherited is not None:
            expected = identity.as_dict()
            if (not isinstance(inherited, dict) or inherited.get("schema") != 1 or inherited.get("identity") != expected
                    or inherited.get("cleanup_grace_seconds") != grace):
                raise runtime.RuntimeFailure("RunBudgetRejected", "inherited run context mismatch")
            start, end = inherited.get("started_unix_ms"), inherited.get("deadline_unix_ms")
            if (type(start) is not int or type(end) is not int or start > now + 5000
                    or end - start <= grace * 1000 or end - start > maximum * 1000):
                raise runtime.RuntimeFailure("RunBudgetRejected", "invalid inherited complete deadline")
        required_remaining = 0 if cleanup else grace * 1000
        if end > created_ms(identity.run_id) + maximum * 1000 + 5000:
            raise runtime.RuntimeFailure("RunBudgetRejected", "deadline exceeds the original identity's run budget")
        protected = record.get("protected_benchmark_roots", []) + [record.get("config", {}).get(
            "benchmark_root", "s3://novarocks/shared/benchmarks")]
        if any(contains(root, identity.scope_root) or contains(identity.scope_root, root) for root in protected):
            raise runtime.RuntimeFailure("RunBudgetRejected", "scratch overlaps a protected benchmark root")
        if end <= now + required_remaining:
            code = "RunBudgetExpired" if inherited is not None else "RunBudgetRejected"
            raise runtime.RuntimeFailure(code, "original fixture deadline has no remaining admitted budget")
        return cls(identity, start, end, grace, monotonic() + (end - now) / 1000, monotonic)

    def remaining_seconds(self, *, work: bool = False) -> float:
        end = self.deadline - (self.cleanup_grace_seconds if work else 0)
        return max(0.0, end - self.monotonic())

    def check(self, *, work: bool = True) -> None:
        if self.remaining_seconds(work=work) <= 0:
            raise runtime.RuntimeFailure("RunBudgetExpired", "original fixture deadline expired")

    def as_dict(self) -> dict:
        return dict(schema=1, identity=self.identity.as_dict(), started_unix_ms=self.started_unix_ms,
                    deadline_unix_ms=self.deadline_unix_ms,
                    cleanup_grace_seconds=self.cleanup_grace_seconds)


class ScratchSweeper:
    def __init__(self, record, backend, *, wall_clock=time.time, monotonic=time.monotonic):
        self.record, self.backend = record, backend
        self.wall_clock, self.monotonic = wall_clock, monotonic
        self.blocked_runs = set()
        self.protected = [canonical_s3(p) for p in record.get("protected_benchmark_roots", [])]
        self.protected.append(canonical_s3(record.get("config", {}).get(
            "benchmark_root", "s3://novarocks/shared/benchmarks")))

    def _check(self, deadline):
        if not math.isfinite(deadline) or self.monotonic() >= deadline:
            raise runtime.RuntimeFailure("ScratchSweepTimeout", "scratch sweep deadline expired")

    def _mc(self, args, deadline, *, absent_ok=False):
        self._check(deadline)
        command = " ".join(shlex.quote(str(a)) for a in ["mc", "--json", *args])
        # Preserve mc's native exit in the protocol even though the Docker
        # wrapper itself must finish successfully to return the structured body.
        script = ('export MC_HOST_store="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000"\n'
                  "set +e\n" + command + " 2>&1\nnr_mc_exit=$?\n"
                  "printf '\\nNOVA_SCRATCH_MC_EXIT=%s\\n' \"$nr_mc_exit\"\nexit 0")
        timed_out = False
        try:
            output = self.backend.run_mc(self.record, script, deadline=deadline)
        except runtime.RuntimeFailure as error:
            if error.code == "OperationDeadlineExceeded":
                raise runtime.RuntimeFailure("ScratchSweepTimeout", "scratch sweep deadline expired before dispatch") from error
            if error.code != "DockerOperationTimeout":
                raise
            timed_out = True
            output = getattr(error, "stdout", None)
            if output is None:
                raise runtime.RuntimeFailure("ScratchSweepTimeout", "scratch mc deadline expired after confirmed cleanup") from error
        if len(output.encode()) > MAX_MC_BYTES:
            raise runtime.RuntimeFailure("InvalidMcResponse", "mc response exceeds byte limit")
        lines = output.splitlines()
        has_status = bool(lines and re.fullmatch(r"NOVA_SCRATCH_MC_EXIT=[0-9]+", lines[-1]))
        if not has_status and not timed_out:
            raise runtime.RuntimeFailure("InvalidMcResponse", "missing native mc exit")
        status = int(lines.pop().split("=", 1)[1]) if has_status else None
        records, missing = [], False
        body = "\n".join(lines).strip()
        decoder = json.JSONDecoder()
        while body:
            try:
                value, consumed = decoder.raw_decode(body)
                body = body[consumed:].lstrip()
            except (TypeError, ValueError) as error:
                if timed_out and not has_status:
                    # A deadline may truncate the final JSON object. Complete
                    # native error objects before it still take precedence.
                    break
                raise runtime.RuntimeFailure("InvalidMcResponse", "non-JSON mc response") from error
            if not isinstance(value, dict):
                raise runtime.RuntimeFailure("InvalidMcResponse", "invalid mc JSON shape")
            if value.get("status") == "error":
                detail = value.get("error")
                cause = detail.get("cause") if isinstance(detail, dict) else None
                native = cause.get("error") if isinstance(cause, dict) else None
                code = native.get("Code") if isinstance(native, dict) else None
                code = code or (detail.get("code") if isinstance(detail, dict) else None)
                if absent_ok and code in MISSING_CODES:
                    missing = True
                    continue
                public = "ScratchAccessDenied" if code in {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"} else "ScratchMcFailed"
                raise runtime.RuntimeFailure(public, "mc reported an operation failure")
            records.append(value)
        if status and not (absent_ok and missing and not records):
            raise runtime.RuntimeFailure("ScratchMcFailed", f"mc exited with {status}")
        if timed_out:
            raise runtime.RuntimeFailure("ScratchSweepTimeout", "scratch mc deadline expired after confirmed cleanup")
        self._check(deadline)
        return records

    def _expired(self, timestamp, expired_only):
        now = int(self.wall_clock() * 1000)
        if timestamp > now + 5000:
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "scratch timestamp is in the future")
        return not expired_only or timestamp < now - self.record["policy"]["scratch_ttl_seconds"] * 1000

    def _protected(self, uri, *, deletion=False):
        return any(contains(root, uri) or (deletion and contains(uri, root)) for root in self.protected)

    def _error(self, report, operation, target, error):
        # Native diagnostics may contain signed URLs or request credentials.
        # The owner retains those privately; public sweep records use typed codes.
        report["complete"] = False
        report["errors"].append(dict(operation=operation, target=target,
                                     code=getattr(error, "code", type(error).__name__)))

    def _observe(self, report, kind, name, run):
        if "_observed" in report:
            report["_observed"][kind, name] = created_ms(run)

    def _unknown(self, report, kind):
        if "unknown_counts" in report:
            report["unknown_counts"][kind] += 1
            report["enumeration_complete"] = False

    def _protected_namespace(self, run):
        # Namespace names alone cannot establish their children's locations.
        # A configured protected path containing the run is enough to exclude it.
        return any(run in s3_parts(root)[1].split("/") for root in self.protected)

    def _iam(self, expired_only, deadline, report, *, disable=False):
        # A crash between user-add and policy-attach leaves policyName empty.
        # Read every controllable policy before inferring an orphan's identity;
        # a successfully read subset never proves a unique reverse mapping.
        facts, candidates, failed_policies = {}, {}, set()
        inventory_complete, fact_bytes, names = True, 0, set()
        try:
            items = self._mc(["admin", "policy", "list", "store"], deadline)
        except runtime.RuntimeFailure as error:
            inventory_complete, items = False, []
            if "enumeration_complete" in report:
                report["enumeration_complete"] = False
            self._error(report, "inspect-policies", "store", error)
            if error.code == "ScratchSweepTimeout":
                raise
        for item in items:
            self._check(deadline)
            name = item.get("policy")
            try:
                if not isinstance(name, str) or name in names:
                    raise runtime.RuntimeFailure("InvalidMcResponse", "IAM policy list has an invalid or duplicate name")
                names.add(name)
                if not resource_run(name):
                    if parse_iam_name(name) is not None:
                        raise runtime.RuntimeFailure("ScratchIamUnmapped", "compact policy has no full run identity")
                    continue
                identity, purpose = self._policy_identity(name, deadline)
                if identity is None:
                    continue
                self._expired(created_ms(identity.run_id), False)
                fact_bytes += len(name.encode()) + len(identity.scope_root.encode())
                if len(facts) >= MAX_PENDING_PREFIXES or fact_bytes > MAX_MC_BYTES:
                    raise runtime.RuntimeFailure("InvalidMcResponse", "IAM policy inventory exceeds its input bound")
                facts[name] = identity, purpose
                candidates.setdefault(identity.iam_name(purpose), set()).add(name)
            except runtime.RuntimeFailure as error:
                inventory_complete = False
                self._unknown(report, "policy")
                self._error(report, "inspect-policy", name if isinstance(name, str) else "unknown", error)
                if error.code == "ScratchSweepTimeout":
                    raise
                if fact_bytes > MAX_MC_BYTES or len(facts) >= MAX_PENDING_PREFIXES:
                    break
        for item in self._mc(["admin", "user", "list", "store"], deadline):
            self._check(deadline)
            name = item.get("accessKey")
            if not isinstance(name, str):
                raise runtime.RuntimeFailure("InvalidMcResponse", "IAM list has no resource name")
            if parse_iam_name(name) is None:
                continue
            policy, classified = None, False
            possible = candidates.get(name, set())
            try:
                rows = self._mc(["admin", "user", "info", "store", name], deadline, absent_ok=True)
                if not rows:
                    continue
                if len(rows) != 1 or rows[0].get("accessKey") != name:
                    raise runtime.RuntimeFailure("InvalidMcResponse", "user info identity mismatch")
                policy, groups = rows[0].get("policyName"), rows[0].get("memberOf")
                if not isinstance(policy, str) or (groups is not None and not isinstance(groups, list)) or groups:
                    raise runtime.RuntimeFailure("ScratchIamUnmapped", "scratch-shaped user has no exact policy mapping")
                if policy == "":
                    if not inventory_complete or len(possible) != 1:
                        raise runtime.RuntimeFailure("ScratchIamUnmapped", "unbound scratch user has no unique complete policy mapping")
                    policy = next(iter(possible))
                    identity, purpose = facts[policy]
                else:
                    if not resource_run(policy):
                        raise runtime.RuntimeFailure("ScratchIamUnmapped", "scratch-shaped user has no exact policy mapping")
                    # Explicit native bindings remain usable after a failed
                    # inventory. They do not depend on orphan inference.
                    identity, purpose = facts.get(policy) or self._policy_identity(policy, deadline)
                    if identity is None:
                        raise runtime.RuntimeFailure("ScratchIamUnmapped", "mapped scratch policy is absent")
                if identity.iam_name(purpose) != name:
                    raise runtime.RuntimeFailure("ScratchIamUnmapped", "scratch user differs from policy identity")
                expired = self._expired(created_ms(identity.run_id), expired_only)
                self._observe(report, "user", name, identity.run_id)
                classified = True
                if not expired:
                    continue
                operation = "disable" if disable else "remove"
                self._mc(["admin", "user", "disable" if disable else "rm", "store", name], deadline, absent_ok=True)
                report["removed"].append(dict(kind="user", name=name, operation=operation))
            except runtime.RuntimeFailure as error:
                if not classified:
                    self._unknown(report, "user")
                # Preserve every validated candidate as well as explicit native
                # bindings. In particular, empty policyName is not a policy ID.
                failed_policies.update(possible)
                if isinstance(policy, str):
                    failed_policies.update(p.strip() for p in policy.split(",") if p.strip())
                self._error(report, "disable" if disable else "remove", name, error)
                if error.code == "ScratchSweepTimeout":
                    raise
        for name, (identity, _) in facts.items():
            self._check(deadline)
            self._observe(report, "policy", name, identity.run_id)
            if disable or not inventory_complete or name in failed_policies:
                continue
            try:
                if self._expired(created_ms(identity.run_id), expired_only):
                    self._mc(["admin", "policy", "rm", "store", name], deadline, absent_ok=True)
                    report["removed"].append(dict(kind="policy", name=name, operation="remove"))
            except runtime.RuntimeFailure as error:
                self._error(report, "remove", name, error)
                if error.code == "ScratchSweepTimeout":
                    raise

    def _policy_identity(self, name, deadline):
        match = RESOURCE_RE.fullmatch(name)
        if not match or len(name) > 63 or match[2] not in {"init", "rot"}:
            raise runtime.RuntimeFailure("ScratchIamUnmapped", "policy does not identify an admitted IAM purpose")
        rows = self._mc(["admin", "policy", "info", "store", name], deadline, absent_ok=True)
        if not rows:
            return None, None
        if len(rows) != 1 or rows[0].get("policy") != name:
            raise runtime.RuntimeFailure("InvalidMcResponse", "policy info identity mismatch")
        info = rows[0].get("policyInfo")
        if not isinstance(info, dict) or info.get("PolicyName") != name:
            raise runtime.RuntimeFailure("InvalidMcResponse", "invalid native policyInfo shape")
        document = info.get("Policy")
        if (not isinstance(document, dict) or document.get("Version") != "2012-10-17"
                or set(document) - {"Version", "Statement"}
                or not isinstance(document.get("Statement"), list) or not document["Statement"]):
            raise runtime.RuntimeFailure("InvalidScratchScope", "invalid scoped IAM policy document")
        object_actions = {"s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject",
                          "s3:DeleteObjectVersion", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"}
        root, bucket_statements = None, []
        for statement in document["Statement"]:
            if (not isinstance(statement, dict) or statement.get("Effect") != "Allow"
                    or set(statement) - {"Effect", "Action", "Resource", "Condition", "Sid"}):
                raise runtime.RuntimeFailure("InvalidScratchScope", "unsupported scoped IAM statement")
            actions, resources = statement.get("Action"), statement.get("Resource")
            actions = [actions] if isinstance(actions, str) else actions
            resources = [resources] if isinstance(resources, str) else resources
            if (not isinstance(actions, list) or not actions or not all(isinstance(a, str) for a in actions)
                    or not isinstance(resources, list) or not resources or not all(isinstance(r, str) for r in resources)):
                raise runtime.RuntimeFailure("InvalidScratchScope", "invalid scoped IAM actions or resources")
            if set(actions) <= object_actions:
                if statement.get("Condition"):
                    raise runtime.RuntimeFailure("InvalidScratchScope", "unexpected object policy condition")
                for resource in resources:
                    if not resource.startswith("arn:aws:s3:::") or not resource.endswith("/*"):
                        raise runtime.RuntimeFailure("InvalidScratchScope", "object policy must name one exact prefix")
                    candidate = canonical_s3("s3://" + resource[len("arn:aws:s3:::"):-2])
                    if "*" in candidate or "?" in candidate or (root is not None and candidate != root):
                        raise runtime.RuntimeFailure("InvalidScratchScope", "policy grants more than one scratch root")
                    root = candidate
            elif set(actions) <= {"s3:ListBucket", "s3:GetBucketLocation"}:
                bucket_statements.append((set(actions), resources, statement.get("Condition")))
            else:
                raise runtime.RuntimeFailure("InvalidScratchScope", "policy action exceeds the scratch contract")
        if root is None or self._protected(root, deletion=True):
            raise runtime.RuntimeFailure("InvalidScratchScope", "policy root is absent or protected")
        identity = ScratchIdentity(match[1], root)
        bucket, key = s3_parts(root)
        for actions, resources, condition in bucket_statements:
            if set(resources) != {"arn:aws:s3:::" + bucket}:
                raise runtime.RuntimeFailure("InvalidScratchScope", "policy bucket differs from the scratch root")
            if "s3:ListBucket" not in actions:
                if condition:
                    raise runtime.RuntimeFailure("InvalidScratchScope", "unexpected location condition")
                continue
            if not isinstance(condition, dict) or len(condition) != 1 or next(iter(condition)) not in {"StringLike", "StringEquals"}:
                raise runtime.RuntimeFailure("InvalidScratchScope", "bucket listing needs an exact prefix condition")
            predicates = next(iter(condition.values()))
            if not isinstance(predicates, dict) or set(predicates) != {"s3:prefix"}:
                raise runtime.RuntimeFailure("InvalidScratchScope", "invalid bucket prefix condition")
            prefixes = predicates["s3:prefix"]
            prefixes = [prefixes] if isinstance(prefixes, str) else prefixes
            if not isinstance(prefixes, list) or not prefixes or any(not isinstance(p, str) or p not in {key + "/", key + "/*"} for p in prefixes):
                raise runtime.RuntimeFailure("InvalidScratchScope", "bucket listing escapes the scratch root")
        return identity, match[2]

    def revoke_users(self):
        deadline = self.monotonic() + getattr(self.backend, "timeout", 90)
        report = dict(complete=True, removed=[], errors=[])
        self._iam(False, deadline, report, disable=True)
        if not report["complete"]:
            raise runtime.RuntimeFailure("ScratchCredentialRevocationFailed", "scratch users were not all disabled")
        return report

    def cleanup_run(self, identity: ScratchIdentity, exact_users_and_policies: list,
                    deadline: float) -> dict:
        """Settle only resources predeclared by this exact original run context.

        Unlike discovery-based TTL collection, this operation can settle a user
        whose creation succeeded before attaching its predeclared policy. An
        unrelated policy/group mapping remains a refusal, never a guessed owner.
        The caller owns the inherited complete budget and actual writer exit.
        """
        report = dict(complete=True, removed=[], errors=[])
        try:
            self._check(deadline)
            if not isinstance(identity, ScratchIdentity) or self._protected(identity.scope_root, deletion=True):
                raise runtime.RuntimeFailure("InvalidScratchScope", "cleanup scope is not an admitted scratch root")
            if not isinstance(exact_users_and_policies, list):
                raise runtime.RuntimeFailure("ScratchIamUnmapped", "cleanup needs predeclared IAM pairs")
            pairs, seen = [], set()
            # Validate the complete caller-owned set before any remote operation.
            for pair in exact_users_and_policies:
                if not isinstance(pair, dict) or set(pair) != {"user", "policy"}:
                    raise runtime.RuntimeFailure("ScratchIamUnmapped", "invalid cleanup IAM pair")
                purpose = next((p for p in ("init", "rot") if pair["policy"] == identity.resource_name(p)
                                and pair["user"] == identity.iam_name(p)), None)
                if purpose is None or purpose in seen:
                    raise runtime.RuntimeFailure("ScratchIamUnmapped", "cleanup pair differs from the original run")
                seen.add(purpose)
                pairs.append(pair)
            verified = []
            # Read every exact mapping/scope before the first disable or remove.
            for pair in pairs:
                rows = self._mc(["admin", "user", "info", "store", pair["user"]], deadline, absent_ok=True)
                if rows:
                    if (len(rows) != 1 or rows[0].get("accessKey") != pair["user"]
                            or rows[0].get("memberOf")
                            or not isinstance(rows[0].get("policyName", ""), str)
                            or rows[0].get("policyName", "") not in {"", pair["policy"]}):
                        raise runtime.RuntimeFailure("ScratchIamUnmapped", "cleanup user has a foreign IAM binding")
                policy_identity, _ = self._policy_identity(pair["policy"], deadline)
                if policy_identity is not None and policy_identity != identity:
                    raise runtime.RuntimeFailure("InvalidScratchScope", "cleanup policy scope differs from the original run")
                verified.append((pair, bool(rows), policy_identity is not None))
            for pair, user_present, policy_present in verified:
                try:
                    if user_present:
                        self._mc(["admin", "user", "disable", "store", pair["user"]], deadline, absent_ok=True)
                        report["removed"].append(dict(kind="user", name=pair["user"], operation="disable"))
                        self._mc(["admin", "user", "rm", "store", pair["user"]], deadline, absent_ok=True)
                        report["removed"].append(dict(kind="user", name=pair["user"], operation="remove"))
                    if policy_present:
                        self._mc(["admin", "policy", "rm", "store", pair["policy"]], deadline, absent_ok=True)
                        report["removed"].append(dict(kind="policy", name=pair["policy"], operation="remove"))
                except runtime.RuntimeFailure as error:
                    self._error(report, "cleanup-iam-pair", pair["user"], error)
                    if error.code == "ScratchSweepTimeout":
                        raise
            # An unrevoked identity may still write. Preserve its data on IAM
            # failure and retain the exact context for an explicit retry.
            if report["complete"]:
                bucket, key = s3_parts(identity.scope_root)
                self._mc(["rm", "--recursive", "--force", "store/" + bucket + "/" + key + "/"], deadline, absent_ok=True)
                report["removed"].append(dict(kind="s3-prefix", uri=identity.scope_root))
        except runtime.RuntimeFailure as error:
            self._error(report, "cleanup-run", getattr(identity, "run_id", "invalid-context"), error)
        return report

    def _s3(self, expired_only, deadline, report):
        # The optional private cursor is only a continuation of enumeration.
        # It records no holder, lease, authority or liveness fact.
        cursor = (Path(self.record["compose_file"]).parent / "scratch-cursor.json"
                  if self.record.get("compose_file") else None)
        pending = []
        cursor_identity = dict(schema=1, version=self.record.get("version"),
                               generation=self.record.get("generation"), expired_only=expired_only)
        if cursor and cursor.is_symlink():
            raise runtime.RuntimeFailure("RuntimeStateUnreadable", "scratch continuation is not a regular owned file")
        if cursor and cursor.exists():
            saved = runtime.read_json(cursor)
            if all(saved.get(k) == v for k, v in cursor_identity.items()):
                pending = saved.get("pending")
                if (not isinstance(pending, list) or len(pending) > MAX_PENDING_PREFIXES
                        or not all(isinstance(p, str) and canonical_s3(p) == p for p in pending)):
                    raise runtime.RuntimeFailure("RuntimeStateUnreadable", "invalid scratch enumeration continuation")
                if pending:
                    report["enumeration_complete"] = False
        if not pending:
            for row in self._mc(["ls", "store"], deadline):
                if row.get("type") != "folder" or not isinstance(row.get("key"), str):
                    raise runtime.RuntimeFailure("InvalidMcResponse", "invalid S3 bucket listing")
                bucket, key = s3_parts("s3://" + row["key"].rstrip("/"))
                if key:
                    raise runtime.RuntimeFailure("InvalidMcResponse", "invalid S3 bucket identity")
                pending.append("s3://" + bucket)
        visited = 0
        try:
            while pending:
                self._check(deadline)
                if visited >= MAX_PREFIX_VISITS:
                    raise runtime.RuntimeFailure("ScratchSweepTraversalLimit", "prefix visit budget exhausted")
                uri = pending[-1]
                visited += 1
                bucket, key = s3_parts(uri)
                parts = key.split("/") if key else []
                if self._protected(uri):
                    pending.pop()
                    continue
                if len(parts) > MAX_PREFIX_DEPTH:
                    report["enumeration_complete"] = False
                    pending.pop()
                    self._error(report, "enumerate-s3-prefix", uri, runtime.RuntimeFailure(
                        "ScratchSweepTraversalLimit", "prefix depth exceeded"))
                    continue
                if len(parts) >= 2 and parts[-2] == "scratch" and RUN_RE.fullmatch(parts[-1]):
                    run = parts[-1]
                    if self._protected(uri, deletion=True):
                        pending.pop()
                        continue
                    expired = self._expired(created_ms(run), expired_only)
                    self._observe(report, "s3-prefix", uri, run)
                    if run in self.blocked_runs or not expired:
                        pending.pop()
                        continue
                    try:
                        self._mc(["rm", "--recursive", "--force", "store/" + bucket + "/" + key + "/"], deadline, absent_ok=True)
                        report["removed"].append(dict(kind="s3-prefix", uri=uri))
                    except runtime.RuntimeFailure as error:
                        self._error(report, "remove-s3-prefix", uri, error)
                        if error.code == "ScratchSweepTimeout":
                            raise
                    pending.pop()
                    continue
                children = []
                target = "store/" + bucket + ("/" + key if key else "") + "/"
                for row in self._mc(["ls", target], deadline, absent_ok=True):
                    self._check(deadline)
                    child = row.get("key")
                    if row.get("type") not in {"folder", "file"} or not isinstance(child, str):
                        raise runtime.RuntimeFailure("InvalidMcResponse", "invalid S3 object listing")
                    if row["type"] == "folder":
                        component = child[:-1] if child.endswith("/") else ""
                        if not component or "/" in component or component in {".", ".."}:
                            raise runtime.RuntimeFailure("InvalidMcResponse", "unsafe listed S3 prefix")
                        child_uri = canonical_s3(uri + "/" + component)
                        if not self._protected(child_uri):
                            children.append(child_uri)
                if len(pending) + len(children) > MAX_PENDING_PREFIXES:
                    raise runtime.RuntimeFailure("ScratchSweepTraversalLimit", "pending prefix budget exhausted")
                pending.pop()
                pending.extend(sorted(set(children), reverse=True))
        finally:
            if cursor:
                runtime.atomic_json(cursor, {**cursor_identity, "pending": pending})

    def _rest_request(self, service, method, path, deadline, *, absent_ok=False):
        self._check(deadline)
        port = self.record["ports"]["rest" if service == "rest" else "rest_mv"]
        connection = _DeadlineConnection(port, deadline, self.monotonic)
        response = None
        try:
            connection.request(method, "/v1/" + path)
            response = connection.getresponse()
            if response.status == 404 and absent_ok:
                return None
            if not 200 <= response.status < 300:
                code = "ScratchAccessDenied" if response.status in (401, 403) else "ScratchRestFailed"
                raise runtime.RuntimeFailure(code, f"REST returned HTTP {response.status}")
            raw = response.read(MAX_REST_BYTES + 1)
            self._check(deadline)
            if len(raw) > MAX_REST_BYTES:
                raise runtime.RuntimeFailure("InvalidRestResponse", "REST listing exceeds byte limit")
            value = json.loads(raw) if raw else {}
            if not isinstance(value, dict):
                raise runtime.RuntimeFailure("InvalidRestResponse", "REST response is not an object")
            return value
        except (TimeoutError, OSError, http.client.HTTPException) as error:
            if self.monotonic() >= deadline:
                raise runtime.RuntimeFailure("ScratchSweepTimeout", "original REST deadline expired") from error
            raise runtime.RuntimeFailure("ScratchRestUnavailable", "REST request did not complete") from error
        except ValueError as error:
            raise runtime.RuntimeFailure("InvalidRestResponse", "REST response is not JSON") from error
        finally:
            if response is not None:
                response.close()
            connection.close()

    def _pages(self, service, path, field, deadline, *, absent_ok=False):
        token, seen = None, set()
        while True:
            query = urlencode({"pageToken": token}) if token else ""
            response = self._rest_request(service, "GET", path + ("?" + query if query else ""), deadline,
                                          absent_ok=absent_ok)
            if response is None:
                return
            if not isinstance(response, dict) or not isinstance(response.get(field), list):
                raise runtime.RuntimeFailure("InvalidRestResponse", "invalid REST list shape")
            yield from response[field]
            token = response.get("next-page-token")
            if token is None:
                return
            if not isinstance(token, str) or not token or token in seen:
                raise runtime.RuntimeFailure("InvalidRestResponse", "invalid REST continuation")
            seen.add(token)

    def _rest(self, deadline, report):
        for service in self.record.get("server_warehouses", {"rest": ""}):
            namespaces = list(self._pages(service, "namespaces", "namespaces", deadline))
            for namespace in namespaces:
                self._check(deadline)
                if not isinstance(namespace, list) or not all(isinstance(n, str) for n in namespace):
                    raise runtime.RuntimeFailure("InvalidRestResponse", "invalid namespace identity")
                if len(namespace) != 1 or not (run := resource_run(namespace[0])):
                    continue
                expired = self._expired(created_ms(run), True)
                if self._protected_namespace(run):
                    continue
                sample_key = (service, namespace[0])
                self._observe(report, "rest-namespace", sample_key, run)
                if not expired:
                    continue
                base = "namespaces/" + quote(namespace[0], safe="")
                try:
                    children = []
                    for collection in ("views", "tables"):
                        for child in self._pages(service, base + "/" + collection, "identifiers", deadline, absent_ok=True):
                            if (not isinstance(child, dict) or child.get("namespace") != namespace
                                    or not isinstance(child.get("name"), str) or not child["name"]):
                                raise runtime.RuntimeFailure("InvalidRestResponse", "invalid child identifier")
                            path = base + "/" + collection + "/" + quote(child["name"], safe="")
                            metadata = self._rest_request(service, "GET", path, deadline, absent_ok=True)
                            if metadata is None:
                                continue
                            facts = metadata.get("metadata") if isinstance(metadata, dict) else None
                            location = facts.get("location") if isinstance(facts, dict) else None
                            if not isinstance(location, str) or self._protected(location, deletion=True):
                                raise runtime.RuntimeFailure("InvalidScratchScope", "REST child has no safe scratch location")
                            _, key = s3_parts(location)
                            if "/scratch/" + run + "/" not in "/" + key + "/":
                                raise runtime.RuntimeFailure("InvalidScratchScope", "REST location belongs to a different run")
                            children.append(path)
                    # Validate every child before deleting the first one.
                    for path in children:
                        self._rest_request(service, "DELETE", path, deadline, absent_ok=True)
                        report["removed"].append(dict(kind="rest-child", service=service, path=path))
                    self._rest_request(service, "DELETE", base, deadline, absent_ok=True)
                    report["removed"].append(dict(kind="rest-namespace", service=service, namespace=namespace))
                except runtime.RuntimeFailure as error:
                    if error.code == "InvalidScratchScope" and "_observed" in report:
                        report["_observed"].pop(("rest-namespace", sample_key), None)
                    self.blocked_runs.add(run)
                    self._error(report, "remove-rest-namespace", namespace[0], error)
                    if error.code == "ScratchSweepTimeout":
                        raise

    def sweep(self, *, expired_only: bool, deadline: float) -> dict:
        kinds = ["s3-prefix", "rest-namespace", "user", "policy"]
        report = dict(complete=True, removed=[], errors=[],
                      observed_counts={kind: 0 for kind in kinds},
                      oldest_created_unix_ms=None, unknown_counts=dict(user=0, policy=0),
                      sampled_at_unix_ms=int(self.wall_clock() * 1000), counts_are_pre_cleanup=True,
                      enumeration_complete=True,
                      enumeration_scope=kinds if expired_only else [kind for kind in kinds if kind != "rest-namespace"],
                      classification_basis={
                          "rest-namespace": "parseable-namespace-name; nonexpired child metadata not inspected",
                          "s3-prefix": "exact run directory; protected roots excluded",
                          "user": "native binding or unique complete policy inventory; exact scoped policy",
                          "policy": "full run policy name and exact scoped policy"},
                      _observed={})
        self.blocked_runs = set()
        try:
            self._check(deadline)
            # Maintenance has stopped REST writers and will delete their exact
            # state volumes. Its S3/IAM purge must not call stopped listeners.
            if expired_only:
                self._rest(deadline, report)
            self._s3(expired_only, deadline, report)
            self._iam(expired_only, deadline, report)
        except runtime.RuntimeFailure as error:
            report["enumeration_complete"] = False
            self._error(report, "enumerate-scratch", "machine", error)
        observed = report.pop("_observed")
        for (kind, _), timestamp in observed.items():
            report["observed_counts"][kind] += 1
            oldest = report["oldest_created_unix_ms"]
            report["oldest_created_unix_ms"] = timestamp if oldest is None else min(oldest, timestamp)
        return report
