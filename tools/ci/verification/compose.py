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
"""Compose coverage, product counterexamples and cleanup obligations separately."""
from collections import defaultdict
import re

from .dispositions import Dispositions
from .identity import same_identity
from .model import Attempt
from .scope import required_scopes


def _target_identity(target, scope, unit=None):
    identities = target.get("identities", target)
    return identities.get(scope.key, identities.get(str(unit), scope.identity))


def _confirmed_cleanup(observation, confirmations):
    for value in confirmations:
        if value.get("schema") != 1 or value.get("attempt") != observation.attempt:
            continue
        # Confirmation must identify the actual obligation; an unrelated check
        # or a merely saved manual observation cannot discharge it.
        if value.get("scope") not in (None, observation.scope):
            continue
        if value.get("lifecycle") not in (None, observation.lifecycle):
            continue
        if value.get("observation") not in (None, observation.ref):
            continue
        if not any(value.get(key) for key in ("scope", "lifecycle", "observation")):
            continue
        checks = value.get("checks", [])
        if (value.get("summary") == "ok" and value.get("complete") is True
                and checks and all(check.get("ok") is True for check in checks)):
            return True
    return False


# Design: ADR-0186 (docs/adr/ADR-0186-layered-verification-and-read-only-evidence-composition.md)
def compose(target, layer, attempts, declarations, dispositions=None, confirmations=()):
    """Return a derived schema-1 record; all input objects remain unchanged.

    ``target`` maps scope keys to execution identities, optionally nested under
    ``identities`` with ``current_attempt``. Declarations determine membership,
    never the observations. A Scope.identity is an optional target fallback.
    """
    scopes = required_scopes(layer, declarations)
    disposition = dispositions if isinstance(dispositions, Dispositions) else Dispositions(dispositions)
    attempts = [value if isinstance(value, Attempt) else Attempt(**value) for value in attempts]
    if len({value.id for value in attempts}) != len(attempts):
        raise ValueError("duplicate attempt identity")
    current = target.get("current_attempt")
    current_ids = {value.id for value in attempts if value.current or value.id == current}
    if layer == "full" and len(current_ids) > 1:
        raise ValueError("full verification has one current attempt")
    if layer == "admission":
        pool = [value for value in attempts if value.formal
                and value.source.get("dirty") is False
                and re.fullmatch(r"[0-9a-f]{40}", value.source.get("commit", ""))]
    elif layer == "full":
        pool = [value for value in attempts if value.id in current_ids]
    else:
        pool = attempts
    result = {"schema": 1, "layer": layer, "target": target,
              "attempts": [{"id": value.id, "formal": value.formal,
                            "current": value.id in current_ids, "source": value.source,
                            "root": value.root, "errors": list(value.errors)} for value in attempts],
              "scopes": [], "layer_ok": True}
    for scope in scopes:
        observations = [obs for attempt in pool for obs in attempt.observations
                        if obs.scope == scope.key and obs.unit in scope.units]
        quarantined = {unit: disposition.excluded(unit) for unit in scope.units if disposition.excluded(unit)}
        excluded = quarantined if layer == "admission" else {}
        gated = [unit for unit in scope.units if unit not in excluded]
        counter, passes, diagnostic = defaultdict(list), defaultdict(list), defaultdict(list)
        records, obligations = [], {}
        for obs in observations:
            matched = (obs.identity_confirmed
                       and same_identity(obs.identity, _target_identity(target, scope, obs.unit)))
            category = "inapplicable"
            if obs.result in {"cancelled", "unknown", "not_run"} or not obs.identity_confirmed:
                category = "coverage_gap"
            elif matched and obs.applicability != "canonical":
                category = "diagnostic"
                diagnostic[obs.unit].append(obs)
            elif matched and obs.result == "fail":
                category = "counterexample"
                counter[obs.unit].append(obs)
            elif matched and obs.result == "pass":
                category = "pass"
                passes[obs.unit].append(obs)
            record = obs.to_dict()
            record.update(category=category,
                          source="current" if obs.attempt in current_ids else "historical",
                          identity_matches=matched,
                          disposition=disposition.match(obs) if matched else None)
            records.append(record)
            if obs.cleanup != "ok" and not _confirmed_cleanup(obs, confirmations):
                key = (obs.attempt, obs.lifecycle, obs.cleanup)
                obligations[key] = {"attempt": obs.attempt, "scope": scope.key,
                                    "lifecycle": obs.lifecycle, "summary": obs.cleanup,
                                    "ref": obs.ref}

        lifecycles = defaultdict(list)
        for obs in observations:
            lifecycles[(obs.attempt, obs.lifecycle)].append(obs)
        eligible = []
        for (attempt_id, lifecycle), execution in sorted(lifecycles.items()):
            # Duplicate terminal records in one lifecycle are ambiguous. They
            # remain observations but cannot establish positive coverage.
            if any(sum(obs.unit == unit for obs in execution) != 1 for unit in gated):
                continue
            coverage = [obs for obs in execution if obs.unit in gated]
            if not coverage:
                continue
            if not all(obs.identity_confirmed and same_identity(
                    obs.identity, _target_identity(target, scope, obs.unit))
                    and obs.record_complete and obs.terminated and obs.result in {"pass", "fail"}
                    and (obs.cleanup == "ok" or _confirmed_cleanup(obs, confirmations))
                    and obs.applicability == "canonical"
                    for obs in coverage):
                continue
            if attempt_id not in current_ids and not (scope.initial_state and scope.inputs_declared
                                                       and all(obs.inputs_declared for obs in coverage)):
                continue
            eligible.append({"attempt": attempt_id, "lifecycle": lifecycle,
                             "source": "current" if attempt_id in current_ids else "historical",
                             "refs": [obs.ref for obs in coverage],
                             "scope": scope.key})
        all_counter = [obs for unit in gated for obs in counter[unit]]
        undisposed = [obs for obs in all_counter if not
                      (layer in {"dev", "admission"} and disposition.match(obs))]
        if not gated:
            verdict = "excluded"
        elif any(counter[unit] and passes[unit] for unit in gated):
            verdict = "inconsistent"
        elif undisposed:
            verdict = "failed"
        elif eligible:
            verdict = "known_failure" if all_counter else "satisfied"
        elif all_counter:
            verdict = "not_run"
        elif any(passes[unit] or diagnostic[unit] for unit in gated):
            verdict = "diagnostic_only"
        elif layer == "dev" and scope.deferred:
            verdict = "deferred"
        else:
            verdict = "not_run"
        eligible.sort(key=lambda value: (value["source"] != "current", value["attempt"], value["lifecycle"]))
        item = {"key": scope.key, "units": [str(unit) for unit in scope.units],
                "gated_units": [str(unit) for unit in gated], "verdict": verdict,
                "excluded": {str(unit): entry for unit, entry in excluded.items()},
                "quarantined": {str(unit): entry for unit, entry in quarantined.items()},
                "observations": sorted(records, key=lambda record: (record["attempt"], record["lifecycle"], record["unit"], record["ref"])),
                "accepted_execution": eligible[0] if eligible else None,
                "eligible_executions": eligible,
                "cleanup_obligations": [obligations[key] for key in sorted(obligations)],
                "stale_known_failures": disposition.stale([
                    obs for obs in observations if obs.identity_confirmed
                    and same_identity(obs.identity, _target_identity(target, scope, obs.unit))
                    and obs.applicability == "canonical"]),
                "deferred": scope.deferred if verdict == "deferred" else None}
        result["scopes"].append(item)
        if verdict not in {"satisfied", "known_failure", "excluded"} or obligations:
            result["layer_ok"] = False
    return result
