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
"""Decide whether independent collection can continue; never dispose failures.

The native readers own identities, execution and cleanup facts. This small
adapter does not define gate membership, consume dispositions, or grant a
successful verification verdict. Its caller retains every raw nonzero exit.
"""
import argparse
from collections import Counter
import json
from pathlib import Path

from .materials import read_invocation, read_json, read_rust, read_sql


def _observations(observations, errors):
    blockers = list(errors)
    if not observations:
        blockers.append("no native terminal observations")
    for observation in observations:
        if not (observation.result in {"pass", "fail"}
                and observation.record_complete and observation.terminated
                and observation.identity_confirmed and observation.cleanup == "ok"
                and observation.applicability == "canonical"):
            blockers.append("incomplete, unstable or unsafe native observation: " + str(observation.unit))
    return blockers


def check(kind, path, raw_exit, invocation_path=None):
    path = Path(path)
    observations, blockers = [], []
    try:
        if kind == "rust":
            index = read_json(path / "execution-index.json")
            if (index.get("complete") is not True or index.get("cancelled") != []
                    or index.get("error") or index.get("source_end_error")
                    or index.get("exit_code") != raw_exit or raw_exit not in (0, 1)):
                blockers.append("Rust batch did not complete normally")
            for name, allowed in (("prepare", (0,)), ("error_manifest", (0,)),
                                  ("nextest", (0, 100)), ("doctest", (0, 101)),
                                  ("final_prepare", (0,))):
                stage = index.get("stages", {}).get(name, {})
                if (stage.get("returncode") not in allowed or stage.get("errors") != []
                        or stage.get("started") is not True):
                    blockers.append("incomplete or abnormal Rust stage: " + name)
            observations, errors = read_rust(path, path.name)
            if not any(value.unit.kind == "rust-test" for value in observations):
                blockers.append("Rust selection has no native tests")
        elif kind == "invocation":
            invocation = read_json(path)
            if (invocation.get("kind") != "system-scenario" or raw_exit not in (0, 1)
                    or invocation.get("exit", {}).get("code") != raw_exit):
                blockers.append("System invocation did not have a normal native exit")
            observations, errors = read_invocation(path, path.parent.parent.name)
            if any((observation.detail.get("evidence") or {}).get("outcome")
                   != ("passed" if raw_exit == 0 else "failed") for observation in observations):
                blockers.append("System native outcome contradicts its exit")
        elif kind == "sql":
            if invocation_path is None:
                observations, errors = read_sql(path, path.stem)
            else:
                invocation_path = Path(invocation_path)
                invocation = read_json(invocation_path)
                ref = invocation.get("materials", {}).get("sql_results")
                referenced = Path(ref) if isinstance(ref, str) else Path("")
                if not referenced.is_absolute():
                    referenced = invocation_path.parent / referenced
                if (invocation.get("kind") != "sql-suite" or not ref
                        or referenced.resolve() != path.resolve()
                        or invocation.get("exit", {}).get("code") != raw_exit):
                    blockers.append("SQL invocation does not bind this native result and exit")
                observations, errors = read_invocation(invocation_path, path.stem)
            records = [json.loads(line) for line in path.read_text().splitlines()]
            end = records[-1] if records else {}
            if (end.get("kind") != "end" or end.get("completed") is not True
                    or end.get("exit_code") != raw_exit or raw_exit not in (0, 1)):
                blockers.append("SQL invocation did not have a complete normal native exit")
        else:
            raise ValueError("unknown collection domain")
        blockers.extend(_observations(observations, errors))
        failures = sum(observation.result == "fail" for observation in observations)
        if (raw_exit == 0 and failures) or (raw_exit != 0 and not failures):
            blockers.append("native case results do not explain the invocation exit")
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        blockers.append(str(error))
    return {"schema": 1, "domain": kind, "material": str(path), "raw_exit_code": raw_exit,
            "safe_to_continue": not blockers,
            "observed_results": dict(Counter(observation.result for observation in observations)),
            "blockers": blockers}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("domain", choices=("rust", "invocation", "sql"))
    parser.add_argument("material", type=Path)
    parser.add_argument("raw_exit", type=int)
    parser.add_argument("--invocation", type=Path, help="Bind SQL to its enclosing native invocation")
    options = parser.parse_args(argv)
    value = check(options.domain, options.material, options.raw_exit, options.invocation)
    print(json.dumps(value, sort_keys=True))
    return 0 if value["safe_to_continue"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
