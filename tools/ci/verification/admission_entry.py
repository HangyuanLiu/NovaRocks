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
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Run one admission attempt; the composition alone determines its verdict.

The GitHub check is authoritative for formal provenance. Environment values in
downloaded or locally manufactured materials never confer that authority.
"""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from .admission import compose_attempt, load_enablement
from .record import write_record

REPO = Path(__file__).resolve().parents[3]
REPOSITORY = "NovaRocks/NovaRocks"
WORKFLOW = ".github/workflows/admission.yml"
SHA = re.compile(r"[0-9a-f]{40}")


# Design: ADR-0187 (docs/adr/ADR-0187-merge-queue-admission-and-current-attempt-evidence.md)
def execution_context(environment):
    """Classify the caller, retaining only declared, non-secret run identity."""
    env = environment
    actions = env.get("GITHUB_ACTIONS") == "true"
    event = env.get("GITHUB_EVENT_NAME")
    ref = env.get("GITHUB_REF")
    identity = {
        "repository": env.get("GITHUB_REPOSITORY"),
        "workflow_ref": env.get("GITHUB_WORKFLOW_REF"),
        "workflow_sha": env.get("GITHUB_WORKFLOW_SHA"),
        "run_id": env.get("GITHUB_RUN_ID"),
        "run_attempt": env.get("GITHUB_RUN_ATTEMPT"),
        "event": event, "ref": ref, "head_sha": env.get("GITHUB_SHA"),
        "base_sha": None,
        "runner": {"os": env.get("RUNNER_OS"), "arch": env.get("RUNNER_ARCH"),
                   "environment": env.get("RUNNER_ENVIRONMENT"),
                   "labels": [env["NOVA_ADMISSION_RUNNER_LABEL"]]
                   if env.get("NOVA_ADMISSION_RUNNER_LABEL") else [],
                   "labels_source": "workflow configuration"},
    }
    if not actions:
        return "local-rehearsal", identity
    if event == "merge_group":
        path = env.get("GITHUB_EVENT_PATH")
        if not path:
            raise ValueError("merge_group requires its event document")
        payload = json.loads(Path(path).read_text())
        group = payload.get("merge_group", {})
        head, base = group.get("head_sha"), group.get("base_sha")
        if (not isinstance(head, str) or not SHA.fullmatch(head)
                or not isinstance(base, str) or not SHA.fullmatch(base)
                or head != env.get("GITHUB_SHA")):
            raise ValueError("merge_group head/base identity is invalid or differs from GITHUB_SHA")
        identity.update(head_sha=head, base_sha=base)
    valid_event = event == "merge_group" or (
        event == "workflow_dispatch" and ref == "refs/heads/main")
    workflow_ref = REPOSITORY + "/" + WORKFLOW + "@" + (ref or "")
    formal = (identity["repository"] == REPOSITORY and valid_event
              and identity["workflow_ref"] == workflow_ref
              and isinstance(identity["workflow_sha"], str)
              and SHA.fullmatch(identity["workflow_sha"])
              and env.get("RUNNER_ENVIRONMENT") == "github-hosted")
    if formal:
        if (not isinstance(identity["head_sha"], str) or not SHA.fullmatch(identity["head_sha"])
                or any(not str(identity[key] or "").isdigit()
                       or int(identity[key]) < 1 for key in ("run_id", "run_attempt"))):
            raise ValueError("formal run requires complete source and positive run identities")
        return "formal-admission", identity
    return "diagnostic", identity


def run_native(repo, environment):
    """Preserve the native terminal and find this invocation's new attempt only."""
    root = Path(repo) / "logs/ci-full"
    before = set(root.iterdir()) if root.is_dir() else set()
    result = subprocess.run(["bash", str(Path(repo) / "tools/ci/local-full-ci.sh"),
                             "--cargo-only"], cwd=repo, env=environment)
    created = sorted(path for path in root.iterdir() if path not in before and path.is_dir()) if root.is_dir() else []
    return created, result.returncode


def _markdown(value):
    return str(value).replace("\n", " ").replace("`", "'")


def summary(record, entry):
    lines = ["# Admission: " + ("PASS (enabled scope)" if record.get("layer_ok") else "FAIL"),
             "", "Evidence: `" + entry["evidence_class"] + "`",
             "", "Source: `" + _markdown(entry["run_identity"].get("head_sha")) + "`",
             "", "Run: `" + _markdown(entry["run_identity"].get("run_id")) + "` / attempt `"
             + _markdown(entry["run_identity"].get("run_attempt")) + "`", ""]
    scopes = record.get("scopes", [])
    enabled = [item for item in scopes if item.get("verdict") != "not_admitted"]
    counts = Counter(item["key"].split(":", 1)[0] for item in enabled)
    lines += ["## Enabled scope", ""]
    lines += ["- " + kind + ": " + str(count) for kind, count in sorted(counts.items())] or ["- No complete inventory"]
    lines += ["", "## Failures and incomplete coverage", ""]
    failures = [item for item in enabled if item.get("verdict") not in {"satisfied", "known_failure", "excluded"}]
    for item in failures:
        lines.append("- `" + _markdown(item["key"]) + "`: " + _markdown(item.get("verdict")))
        for obs in item.get("observations", []):
            if obs.get("result") != "pass" and obs.get("ref"):
                lines.append("  - Materials: `" + _markdown(obs["ref"]) + "`")
    if not failures:
        lines.append("- None observed")
    lines += ["", "## Known failures and quarantine", ""]
    dispositions = []
    for item in scopes:
        for unit, value in item.get("quarantined", {}).items():
            dispositions.append("- Quarantine `" + _markdown(unit) + "`: " + _markdown(value))
        for obs in item.get("observations", []):
            if obs.get("disposition"):
                dispositions.append("- Known failure `" + _markdown(obs.get("unit")) + "`: " + _markdown(obs["disposition"]))
    lines += list(dict.fromkeys(dispositions)) or ["- None"]
    lines += ["", "## Admission gaps", ""]
    gaps = record.get("admission_gaps", {})
    pending = gaps.get("not_admitted", [])
    for item in pending:
        lines.append("- Not admitted: `" + _markdown(item.get("key", item.get("scope", item)))
                     + "` (" + _markdown(item.get("reason", "pending")) + ")")
    for item in gaps.get("enumeration", []):
        lines.append("- Enumeration gap: `" + _markdown(item.get("name", item)) + "`: " + _markdown(item.get("reason", "")))
    for item in gaps.get("unenumerated", []):
        lines.append("- Unenumerated in this attempt: " + _markdown(item))
    if not pending and not gaps.get("enumeration") and not gaps.get("unenumerated"):
        lines.append("- None")
    lines += ["", "## Cleanup obligations", ""]
    obligations = [value for item in scopes for value in item.get("cleanup_obligations", [])]
    lines += ["- " + _markdown(value) for value in obligations] or ["- None recorded"]
    lines += ["", "## Execution and materials", "",
              "- Native exit: `" + str(entry.get("native_exit")) + "`",
              "- Native attempt: `" + _markdown(entry.get("attempt_root")) + "`",
              "- Composition: `composition.json`", "- Entry: `entry.json`"]
    if entry.get("error"):
        lines += ["- Entry error: " + _markdown(entry["error"])]
    if entry["evidence_class"] != "formal-admission":
        lines += ["", "This rehearsal/diagnostic is not formal admission evidence."]
    return "\n".join(lines) + "\n"


def run_admission(output, repo=REPO, environment=None):
    output = Path(output).expanduser()
    if not output.is_absolute():
        raise ValueError("admission output must be an absolute path")
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    environment = dict(os.environ if environment is None else environment)
    declaration = Path(repo) / "tools/ci/baselines/admission-scope.json"
    entry = {"schema": 1, "evidence_class": "local-rehearsal", "run_identity": {},
             "attempt_root": None, "native_exit": None,
             "enablement": {"path": str(declaration), "sha256": None}}
    code = 2
    try:
        from hashlib import sha256
        if declaration.is_file():
            entry["enablement"]["sha256"] = sha256(declaration.read_bytes()).hexdigest()
        load_enablement(declaration)
        evidence_class, run_identity = execution_context(environment)
        entry.update(evidence_class=evidence_class, run_identity=run_identity)
        created, native_exit = run_native(repo, environment)
        entry.update(native_exit=native_exit, attempt_candidates=[str(path) for path in created])
        if len(created) != 1:
            raise ValueError("native CI did not produce exactly one new attempt directory")
        attempt = created[0]
        entry["attempt_root"] = str(attempt)
        record = compose_attempt(repo, attempt, declaration, run_identity, output,
                                 evidence_class=evidence_class)
        code = 0 if record["layer_ok"] else 1
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        entry["error"] = str(error)
        record = {"schema": 1, "layer": "admission", "layer_ok": False,
                  "evidence_class": entry["evidence_class"], "scopes": [],
                  "errors": [str(error)],
                  "admission_gaps": {"not_admitted": [], "enumeration": [],
                                     "unenumerated": ["native inventory/entry incomplete: " + str(error)]}}
        if not (output / "composition.json").exists():
            write_record(output / "composition.json", record)
    entry["exit_code"] = code
    write_record(output / "entry.json", entry)
    with (output / "summary.md").open("x", encoding="utf-8") as stream:
        stream.write(summary(record, entry))
    return code


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        return run_admission(args.output)
    except (OSError, ValueError) as error:
        print("error: " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
