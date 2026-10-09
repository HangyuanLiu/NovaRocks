#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Optional frozen small Native HMS correctness preflight.

The stock helper and its original four phase programs remain unchanged. The
optional native mode inserts a standard 1FE+3BE runner and a fresh stock oracle
between the original independent oracle and drop. Abnormal Native outcomes
retain every dependent owner; host/runner PGID exit never proves role exit.
"""
import argparse
import errno
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import sys
import time
import tomllib
from pathlib import Path

LOCAL_CAP = 1_048_576
BLOCK_BYTES = 65_536
BASE_RELATIVE = "docs/testing/mem-1-m07/scripts/prepare_real_hms_capability.py"


def bounded_file(path):
    path = Path(path)
    before = path.stat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > LOCAL_CAP:
        raise ValueError("local preflight input is not a bounded regular file")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns):
            raise ValueError("local preflight input changed before read")
        result = stream.read(LOCAL_CAP + 1)
    if len(result) != before.st_size:
        raise ValueError("local preflight input changed during read")
    return result


def decode_closed(data):
    def object_hook(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("JSON input contains duplicate keys")
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=object_hook)


def load_base(repo, capability_freeze):
    path = repo / BASE_RELATIVE
    data = bounded_file(path)
    if hashlib.sha256(data).hexdigest() != capability_freeze["helper_sha256"]:
        raise ValueError("original stock helper source differs")
    specification = importlib.util.spec_from_file_location("m07_stock_hms_capability", path)
    module = importlib.util.module_from_spec(specification)
    exec(compile(data, str(path), "exec"), module.__dict__)
    if module.REPO.resolve() != repo:
        raise ValueError("original helper repository differs")
    module.validate_freeze(capability_freeze)
    return module


def stream_pin(base, path, expected_bytes, expected_sha256, deadline):
    before = Path(path).stat()
    base.need(stat.S_ISREG(before.st_mode) and type(expected_bytes) is int and
        0 < expected_bytes == before.st_size, "native artifact length differs before growth")
    digest, total = hashlib.sha256(), 0
    with Path(path).open("rb") as stream:
        opened = os.fstat(stream.fileno())
        identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
        base.need(identity(before) == identity(opened), "native artifact changed before hash")
        while total < expected_bytes:
            base.remaining(deadline)
            block = stream.read(min(BLOCK_BYTES, expected_bytes - total))
            base.need(bool(block), "native artifact ended before frozen length")
            digest.update(block)
            total += len(block)
        base.need(not stream.read(1) and identity(before) == identity(os.fstat(stream.fileno())),
            "native artifact changed during hash")
    base.need(digest.hexdigest() == expected_sha256, "native artifact digest differs")
    base.remaining(deadline)


def source_tree_digest(expected_revision):
    """Private read-only child mode: hash raw Git output without retaining it.

    The outer existing capture owns this wrapper group. Git inherits that same
    group, and every child is synchronously waited; no external service starts.
    """
    digest = hashlib.sha256()
    for args, trim in ((["rev-parse", "HEAD"], True), (["status", "--porcelain=v1"], True),
            (["ls-files", "-s"], False), (["diff", "--binary", "HEAD"], False),
            (["diff", "--binary", "--cached"], False)):
        child = subprocess.Popen(["git", *args], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            if trim:
                value = child.stdout.read(LOCAL_CAP + 1)
                if len(value) > LOCAL_CAP:
                    raise ValueError("Git identity output exceeds frozen bound")
                value = value.strip()
                if args[0] == "rev-parse" and value.decode() != expected_revision:
                    raise ValueError("source revision differs")
                if args[0] == "status" and value:
                    raise ValueError("source is dirty")
                digest.update(value)
            else:
                while True:
                    block = child.stdout.read(BLOCK_BYTES)
                    if not block:
                        break
                    digest.update(block)
            if child.wait() != 0:
                raise ValueError("Git identity command failed")
            digest.update(b"\0")
        finally:
            child.stdout.close()
            if child.poll() is None:
                child.kill()
                child.wait()
    return digest.hexdigest()


def validate_native_freeze(base, frozen, capability_freeze, capability_bytes):
    base.exact_keys(frozen, ("schema_version", "task", "spec_revision", "purpose", "review_status",
        "frozen_before_execution", "source_revision", "source_tree_sha256", "cargo_lock_sha256",
        "runner_native_build_identity", "runner_path", "runner_bytes", "runner_sha256", "server_path",
        "server_bytes", "server_sha256", "build_receipt_path", "build_receipt_sha256",
        "original_capability_execution_freeze_sha256", "companion_helper_sha256", "bounds", "input"))
    base.need(frozen["schema_version"] == 1 and frozen["task"] == "MEM-1-M07" and frozen["spec_revision"] == 7 and
        frozen["purpose"] == "private-stock-hms-small-native-classification-only" and
        frozen["review_status"] == "reviewed" and frozen["frozen_before_execution"] is True,
        "optional native freeze is not reviewed and frozen")
    base.need(frozen["source_revision"] == capability_freeze["source_revision"] and
        frozen["original_capability_execution_freeze_sha256"] == base.sha(capability_bytes),
        "optional native parent/source identity differs")
    base.need(frozen["companion_helper_sha256"] == base.sha(bounded_file(Path(__file__))),
        "optional companion source differs")
    base.need(frozen["input"] == capability_freeze["input"], "optional native changed original small capability input")
    base.need(frozen["bounds"] == {**capability_freeze["bounds"], "native_stage_seconds":240,
        "startup_timeout_seconds":30, "role_exit_reserve_seconds":24, "exact_role_count":4,
        "exact_backend_count":3, "assertion_budget_millis_max":216000}, "optional native bounds differ")
    for key in ("source_revision", "source_tree_sha256", "cargo_lock_sha256", "runner_sha256", "server_sha256",
            "build_receipt_sha256", "original_capability_execution_freeze_sha256", "companion_helper_sha256"):
        count = 40 if key == "source_revision" else 64
        base.need(isinstance(frozen[key], str) and len(frozen[key]) == count and
            all(character in "0123456789abcdef" for character in frozen[key]), "native source hash is malformed")
    for key in ("runner_path", "server_path", "build_receipt_path"):
        base.need(Path(frozen[key]).is_absolute() and str(Path(frozen[key]).resolve(strict=True)) == frozen[key],
            "native artifact path is not canonical")
    base.need(isinstance(frozen["runner_native_build_identity"], str) and
        0 < len(frozen["runner_native_build_identity"].encode()) <= 256, "native build identity is malformed")


def credential(base, config, purpose):
    records = config.get("connector", {}).get("credentials", [])
    selected = [value for value in records if value.get("purpose") == purpose]
    base.need(len(records) == 1 and len(selected) == 1, "published role credentials differ")
    value = selected[0]
    base.exact_keys(value, ("purpose", "name", "generation", "kind", "access_key_id", "access_key_secret"))
    base.need(value == {"purpose":purpose, "name":"iceberg-test-data", "generation":"v1", "kind":"s3",
        "access_key_id":"${ENV:AWS_S3_ACCESS_KEY_ID}", "access_key_secret":"${ENV:AWS_S3_SECRET_ACCESS_KEY}"},
        "published role credential references differ")
    return value


def combined_config(base, owner):
    fe_bytes = base.bounded_read(owner.publication / "fe.toml")
    be_bytes = base.bounded_read(owner.publication / "be.toml")
    fe, be = tomllib.loads(fe_bytes.decode()), tomllib.loads(be_bytes.decode())
    metadata = credential(base, fe, "object-store-metadata")
    data = credential(base, be, "object-store-data")
    base.need({key:value for key,value in metadata.items() if key != "purpose"} ==
        {key:value for key,value in data.items() if key != "purpose"}, "published role credential material differs")
    # Start from the exact FE publication; append only the exact BE credential.
    # Harness rendering projects each role and replaces its owned topology keys.
    suffix = "\n[[connector.credentials]]\n" + "".join(
        key + " = " + json.dumps(value) + "\n" for key,value in data.items())
    combined = fe_bytes + suffix.encode()
    base.need(len(combined) <= owner.bounds["max_local_file_bytes"], "combined base config exceeds bound")
    path = owner.root / "native-combined.toml"
    path.write_bytes(combined)
    path.chmod(0o600)
    return path, base.sha(combined)


SCENARIO = "catalog/mem-1-m07-hms-classification-preflight"
SCENARIO_DIRECTORY = SCENARIO.replace("/", "-")
EXACT_ROLES = {"fe", "be-0", "be-1", "be-2"}
ROLE_EXIT_RESERVE_SECONDS = 24
NATIVE_STAGE_SECONDS = 240
STARTUP_TIMEOUT_SECONDS = 30


def retain_native_owner(owner, reason):
    # Sticky even if the runner's own host group is later proven gone.
    owner.status["resource_retained_whole_failure"] = True
    owner.status.setdefault("retention_barriers", []).append({
        "operation": "small-native-hms", "reason": reason,
        "dependent_owner_retained": True,
        "unknown_native_role_owner_retained": True,
    })
    owner.status["native_role_settlement"] = "unconfirmed; do not run Java/drop/fixture cleanup"


def independently_absent(pid):
    # Presence (including PID reuse) or EPERM is conservative refusal. A recorded
    # PID is never signal authority; only zero signal is used for observation.
    try:
        os.kill(pid, 0)
    except OSError as error:
        return error.errno == errno.ESRCH
    return False


def validate_settled_evidence(base, evidence, code, frozen, base_config_sha256):
    base.need(type(code) is int and code in (0, 1), "native runner exit is not a normal classified return")
    expected_outcome = "passed" if code == 0 else "failed"
    base.need(evidence.get("schema_version") == 5 and evidence.get("scenario") == SCENARIO and
        evidence.get("outcome") == expected_outcome and evidence.get("exit_code") == code,
        "native runner evidence outcome differs")
    base.need(evidence.get("source_revision") == frozen["source_revision"] and
        evidence.get("source_dirty") is False and evidence.get("source_tree_sha256") == frozen["source_tree_sha256"] and
        evidence.get("runner_native_build_identity") == frozen["runner_native_build_identity"] and
        evidence.get("runner_executable") == frozen["runner_path"] and
        evidence.get("primary_binary") == frozen["server_path"] and
        evidence.get("cargo_lock_sha256") == frozen["cargo_lock_sha256"] and
        evidence.get("base_config_sha256") == base_config_sha256 and
        evidence.get("cluster_size") == 3 and evidence.get("launch_profile") == "fault-scenario",
        "native runner source/binary/config identity differs")
    identities = evidence.get("process_launch_identities")
    base.need(isinstance(identities, list) and len(identities) == 4, "native role ledger is missing or partial")
    roles, pids = set(), set()
    for identity in identities:
        base.exact_keys(identity, ("role", "pid", "process_start_token"))
        role, pid, token = identity["role"], identity["pid"], identity["process_start_token"]
        base.need(role in EXACT_ROLES and role not in roles and type(pid) is int and
            0 < pid <= 2_147_483_647 and pid not in pids and isinstance(token, str) and
            0 < len(token.encode()) <= 256, "native exact launch identity is malformed")
        roles.add(role)
        pids.add(pid)
    base.need(roles == EXACT_ROLES, "native role ledger is not exact 1FE+3BE")
    # Complete structured identities come only from the harness's launch facts.
    # A partial stdout/log, parent reap, timeout or absent runner PGID cannot mint them.
    base.need(all(independently_absent(pid) for pid in pids), "native role PID absence was not independently confirmed")
    return identities


def native_runner_step(base, owner, frozen, binding, base_config, artifact_root):
    """Return passed/failed only after normal capture AND exact four-PID absence.

    frozen is independently validated before fixture growth by the companion:
    exact source/clean tree, runner/server bytes+stream hashes+build receipt,
    Cargo.lock/helper/template pins, original capability freeze SHA and bounds.
    binding contains public, owner-derived facts only. AWS values are supplied
    from the already checked private manifest through exact environment names.
    """
    phase_deadline = min(owner.work_deadline, time.monotonic() + NATIVE_STAGE_SECONDS)
    remaining = phase_deadline - time.monotonic()
    base.need(remaining > ROLE_EXIT_RESERVE_SECONDS, "native stage has no role-exit reserve")
    binding = dict(binding)
    binding["assertion_budget_millis"] = min(216_000,
        int((remaining - ROLE_EXIT_RESERVE_SECONDS) * 1000))
    binding_path = owner.root / "native-bound-input.json"
    base.atomic_json(binding_path, binding)
    args = [frozen["runner_path"], "--binary", frozen["server_path"],
        "--config", str(base_config), "--artifact-root", str(artifact_root),
        "--cluster-size", "3", "--timeout-secs", str(STARTUP_TIMEOUT_SECONDS),
        "--launch-profile", "fault-scenario", "--hms-classification-binding", str(binding_path)]
    owner.status["native_started"] = True
    owner.status["native_role_settlement"] = "pending exact four-role evidence"
    try:
        # Existing verifier-wrapper proves only its directly owned host group.
        # It never inherits owner-wrapper's normal-exit=>detached-child proof.
        # A normal code 1 can still have complete independent role settlement;
        # every abnormal capture remains sticky before that separate gate.
        code, partial, exit_facts = base.capture(args, owner.env, phase_deadline,
            owner.bounds["max_output_bytes"], owned_children=owner.children,
            wall_deadline=owner.wall_deadline, reap_seconds=owner.bounds["host_reap_seconds"],
            ownership="verifier-wrapper")
        owner.status["commands"].append({"operation":"small-native-hms", **exit_facts,
            "command_domain":"native-role-wrapper",
            "output_bytes":len(partial), "output_sha256":base.sha(partial),
            "output_scope":"diagnostic only; no role identity authority"})
        base.need(exit_facts["capture_completed"] and exit_facts["leader_reaped"] and
            exit_facts["group_exit_confirmed"] and not exit_facts["resource_retained_whole_failure"],
            "native runner capture did not complete normally")
        evidence_path = Path(artifact_root) / SCENARIO_DIRECTORY / "scenario-evidence.json"
        evidence_bytes = base.bounded_read(evidence_path, owner.bounds["max_local_file_bytes"])
        evidence = base.decode_json(evidence_bytes)
        identities = validate_settled_evidence(base, evidence, code, frozen, binding["base_config_sha256"])
        base.remaining(phase_deadline)
        receipt = {"normal_runner_return":code, "role_settlement":"four exact PIDs independently absent",
            "launch_identities":identities, "scenario_evidence_sha256":base.sha(evidence_bytes),
            "classification_passed":code == 0,
            "mutation_rpc_count":"OPEN; source/component pre-first-mutation guarantee is separate"}
        owner.status["native_role_settlement"] = receipt["role_settlement"]
        owner.status["small_native_classification"] = receipt
        base.atomic_json(owner.root / "small-native-receipt.json", receipt)
        return receipt
    except BaseException as error:
        # Never clear a barrier using a later empty host-children list or reap.
        retain_native_owner(owner, "abnormal capture or incomplete exact native evidence")
        if isinstance(error, base.CaptureFailure):
            owner.status["commands"].append({"operation":"small-native-hms-capture-failure",
                "command_domain":"native-role-wrapper",
                **error.safe_facts(), "output_bytes":len(error.output),
                "output_sha256":base.sha(error.output), "output_scope":"diagnostic only"})
        raise


def fresh_java_oracle(base, owner, prior, baseline):
    """A new stock JVM invocation, using the original read-only oracle program."""
    base.need(owner.status.get("native_role_settlement") == "four exact PIDs independently absent" and
        not owner.status["resource_retained_whole_failure"], "native role settlement has no cleanup authority")
    original_root = owner.root
    owner.root = original_root / "after-native"
    owner.root.mkdir(exist_ok=False)
    try:
        # Reuse the oracle phase protocol/template unchanged. Its old writer
        # exited before native; a distinct actual container ID is saved here.
        fresh = owner.stage("oracle")
        base.need(base.name_set(base.one(fresh, "namespaces"), owner.bounds["max_namespaces"]) ==
            baseline | {owner.namespace}, "fresh Java namespaces changed")
        for kind, count in (("tables", 1), ("views", 1), ("all_objects", 2)):
            base.need(base.one(fresh, kind) == base.one(prior, kind), "fresh Java object listing changed")
        for kind in ("table", "view"):
            base.validate_facts(base.one(fresh, kind), kind, owner.namespace,
                owner.hms["hms"]["warehouse"], owner.freeze)
            base.need(base.one(fresh, kind) == base.one(prior, kind), "fresh Java UUID/location/schema/raw metadata changed")
        return fresh
    finally:
        owner.root = original_root


def make_native_preflight_class(base, native_freeze):
    class NativePreflight(base.Preflight):
        def __init__(self, freeze, output):
            super().__init__(freeze, output)
            self.native_freeze = native_freeze
            self.native_result = None
            self.prior_oracle = None
            self.baseline = None
            self.status["scope"] = "private-stock-hms-small-native-classification-only"

        def command_ownership(self, args, label):
            if label == "native-source-tree-wrapper":
                base.need(args == [sys.executable, str(Path(__file__).resolve()), "--source-tree-only",
                    "--expected-revision", self.native_freeze["source_revision"]], "source wrapper command differs")
                return "git-readonly"
            return super().command_ownership(args, label)

        def native_source_precheck(self):
            frozen = self.native_freeze
            _, source_tree = self.command([sys.executable, str(Path(__file__).resolve()), "--source-tree-only",
                "--expected-revision", frozen["source_revision"]], 15, "native-source-tree-wrapper")
            base.need(source_tree.decode().strip() == frozen["source_tree_sha256"], "actual source tree identity differs")
            base.need(base.sha(base.bounded_read(base.REPO / "Cargo.lock")) == frozen["cargo_lock_sha256"],
                "actual Cargo lock differs")
            for kind in ("runner", "server"):
                stream_pin(base, frozen[kind + "_path"], frozen[kind + "_bytes"],
                    frozen[kind + "_sha256"], self.work_deadline)
            build_bytes = base.bounded_read(Path(frozen["build_receipt_path"]))
            base.need(base.sha(build_bytes) == frozen["build_receipt_sha256"], "native build receipt digest differs")
            build = base.decode_json(build_bytes)
            base.exact_keys(build, ("schema_version", "source_revision", "source_dirty", "source_tree_sha256",
                "cargo_lock_sha256", "runner_native_build_identity", "runner_sha256", "server_sha256"))
            base.need(build == {"schema_version":1, "source_revision":frozen["source_revision"], "source_dirty":False,
                **{key:frozen[key] for key in ("source_tree_sha256", "cargo_lock_sha256",
                    "runner_native_build_identity", "runner_sha256", "server_sha256")}},
                "native build receipt identity differs")
            # This reviewed receipt belongs to the clean serial build owner;
            # this helper never builds, guesses a source/binary relation or downloads.

        def source_precheck(self):
            super().source_precheck()
            self.native_source_precheck()

        def run_native(self):
            # Recheck binaries/source after the external preparation, before launch.
            self.native_source_precheck()
            config, config_sha256 = combined_config(base, self)
            binding = {"schema_version":1,
                "parent_freeze_sha256":self.status["freeze_file_sha256"],
                "pre_native_oracle_sha256":base.sha(base.bounded_read(self.root / "oracle-receipt.json")),
                "publication_sha256":self.status["binding"]["manifest_sha256"],
                "hms_manifest_sha256":self.status["binding"]["hms_manifest_sha256"],
                "base_config_sha256":config_sha256, "catalog_name":self.freeze["input"]["catalog_name"],
                "namespace":self.namespace, "hms_uri":self.hms["hms"]["uri"],
                "warehouse":self.hms["hms"]["warehouse"], "object_store_endpoint":self.manifest["minio"]["endpoint"]}
            artifacts = self.root / "native-artifacts"
            artifacts.mkdir(exist_ok=False)
            names = ("AWS_S3_ACCESS_KEY_ID", "AWS_S3_SECRET_ACCESS_KEY")
            old = {name:self.env.get(name) for name in names}
            self.env.update(AWS_S3_ACCESS_KEY_ID=self.manifest["minio"]["access_key_id"],
                AWS_S3_SECRET_ACCESS_KEY=self.manifest["minio"]["secret_access_key"])
            try:
                return native_runner_step(base, self, self.native_freeze, binding, config, artifacts)
            finally:
                for name,value in old.items():
                    if value is None:
                        self.env.pop(name, None)
                    else:
                        self.env[name] = value

        def stage(self, phase):
            if phase == "drop" and self.native_result is None:
                self.native_result = self.run_native()
                fresh_java_oracle(base, self, self.prior_oracle, self.baseline)
            records = super().stage(phase)
            if phase == "create":
                self.baseline = base.name_set(base.one(records, "baseline"), self.bounds["max_namespaces"])
            elif phase == "oracle" and self.prior_oracle is None:
                self.prior_oracle = records
            return records

        def run_stages(self):
            # Original create/oracle validations occur before the drop hook;
            # original drop/restored occur after settled Native + fresh oracle.
            super().run_stages()
            base.need(self.native_result is not None, "optional native step was not run")
            self.status["capability"]["native_mixed_object_classification"] = self.native_result
            self.status["capability"]["rust_hms_views"] = "exact Unsupported plus independent Java final metadata retained"
            base.need(self.native_result["classification_passed"], "small native classification failed; normal exited owners were cleaned")
    return NativePreflight


def parse_arguments(arguments):
    class UniqueStore(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            if getattr(namespace, self.dest, None) is not None:
                parser.error("preflight option must appear exactly once: " + option_string)
            setattr(namespace, self.dest, values)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", required=True, action=UniqueStore)
    parser.add_argument("--native-freeze", action=UniqueStore)
    parser.add_argument("--output", required=True, action=UniqueStore)
    return parser.parse_args(arguments)


def main():
    args = parse_arguments(sys.argv[1:])
    os.umask(0o077)
    repo = Path.cwd().resolve(strict=True)
    capability_path = Path(args.freeze).resolve(strict=True)
    capability_bytes = bounded_file(capability_path)
    capability = decode_closed(capability_bytes)
    base = load_base(repo, capability)
    if args.native_freeze is None:
        # The original helper remains the authority for its unchanged mode.
        sys.argv = [str(repo / BASE_RELATIVE), "--freeze", str(capability_path), "--output", args.output]
        return base.main()
    native_path = Path(args.native_freeze).resolve(strict=True)
    native_bytes = bounded_file(native_path)
    native = decode_closed(native_bytes)
    validate_native_freeze(base, native, capability, capability_bytes)
    output = Path(args.output).resolve()
    base.need(output.is_relative_to(repo / "logs/mem-1-m07") and
        base.re.fullmatch(r"[a-z0-9_-]+", output.name), "new optional output escaped private artifact root")
    output.mkdir(exist_ok=False)
    run = make_native_preflight_class(base, native)(capability, output)
    run.status.update(freeze_file_path=str(capability_path), freeze_file_sha256=base.sha(capability_bytes),
        native_freeze_file_path=str(native_path), native_freeze_file_sha256=base.sha(native_bytes))
    passed = False
    try:
        run.source_precheck()
        run.bind_owner()
        run.run_stages()
        base.remaining(run.work_deadline)
        passed = True
    except BaseException as error:
        run.retain_cancellation("optional-preflight", error)
        run.status["errors"].append({"class":type(error).__name__,
            "message":str(error) if isinstance(error, base.Refusal) else "optional preflight failure; no raw provider output saved"})
    finally:
        try:
            run.cleanup()
        except BaseException as error:
            run.retain_cancellation("optional-cleanup", error)
            run.status["cleanup_errors"].append({"operation":"optional-cleanup", "class":type(error).__name__})
        passed = (passed and run.status["cleanup_complete"] and not run.status["cleanup_errors"] and
            not run.children and not run.status["resource_retained_whole_failure"])
        run.status["unconfirmed_host_children"] = [child.m07_exit_facts for child in run.children]
        run.status["status"] = "NATIVE_CLASSIFICATION_PREFLIGHT_PASS" if passed else "NATIVE_CLASSIFICATION_PREFLIGHT_FAILED"
        try:
            base.atomic_json(output / "status.json", run.status)
            if passed:
                base.atomic_json(output / "NATIVE_CLASSIFICATION_PREFLIGHT_PASS.json", {
                    "status":run.status["status"], "status_sha256":base.sha(base.bounded_read(output / "status.json")),
                    "scope":run.status["scope"]})
            print(json.dumps({"status":run.status["status"], "output":str(output)}, sort_keys=True))
        except BaseException as error:
            passed = False
            run.retain_cancellation("optional-final-receipt", error)
            run.status["status"] = "NATIVE_CLASSIFICATION_PREFLIGHT_FAILED"
            run.status["errors"].append({"operation":"optional-final-receipt", "class":type(error).__name__})
            try:
                (output / "NATIVE_CLASSIFICATION_PREFLIGHT_PASS.json").unlink(missing_ok=True)
                base.atomic_json(output / "status.json", run.status)
            except BaseException as secondary:
                run.retain_cancellation("optional-failed-receipt", secondary)
    return 0 if passed else 1


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--source-tree-only" and sys.argv[2] == "--expected-revision":
        try:
            print(source_tree_digest(sys.argv[3]))
            exit_code = 0
        except BaseException as error:
            print(json.dumps({"status":"SOURCE_IDENTITY_FAILED", "class":type(error).__name__}))
            exit_code = 1
    else:
        try:
            exit_code = main()
        except BaseException as error:
            print(json.dumps({"status":"OPTIONAL_PRECHECK_FAILED", "class":type(error).__name__}))
            exit_code = 1
    sys.exit(exit_code)
