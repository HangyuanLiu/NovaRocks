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
"""Pure full-verification registration derivation; persistence belongs to G17."""
from copy import deepcopy


def derive(registrations, full_composition, fix_links=()):
    if full_composition.get("schema") != 1 or full_composition.get("layer") != "full":
        raise ValueError("registration derivation requires a full composition")
    records = deepcopy(registrations)
    delta = {key: [] for key in ("new", "still_failing", "last_pass_open", "closed", "infra", "quarantine")}
    for scope in full_composition["scopes"]:
        key, verdict = scope["key"], scope["verdict"]
        for obligation in scope["cleanup_obligations"]:
            delta["infra"].append(deepcopy(obligation))
        quarantined = scope.get("quarantined", scope.get("excluded", {}))
        if quarantined:
            delta["quarantine"].append({"scope": key, "units": list(quarantined),
                                       "observations": [deepcopy(item) for item in scope["observations"]
                                                        if item["unit"] in quarantined]})
        negative = [item for item in scope["observations"] if item.get("category") == "counterexample"]
        if verdict in {"failed", "inconsistent"} and negative and all(item["unit"] in quarantined for item in negative):
            continue
        opened = next((item for item in records if item["scope"] == key and item["status"] == "open"), None)
        evidence = {"verdict": verdict, "target": deepcopy(full_composition["target"]),
                    "observations": deepcopy(scope["observations"]),
                    "execution": deepcopy(scope["accepted_execution"])}
        if verdict in {"failed", "inconsistent"}:
            if opened is None:
                opened = {"scope": key, "status": "open", "observations": [],
                          "interval": {"last_pass": next((item.get("last_pass") for item in reversed(records)
                                                          if item["scope"] == key), None),
                                       "first_failure": deepcopy(full_composition["target"])}}
                records.append(opened)
                delta["new"].append(key)
            else:
                delta["still_failing"].append(key)
            opened["observations"].append(evidence)
        elif verdict == "satisfied" and opened:
            opened["observations"].append(evidence)
            opened["last_pass"] = deepcopy(full_composition["target"])
            delta["last_pass_open"].append(key)
            # An externally supplied ancestry/reverification link is required.
            # A timestamp or an unassociated passing run never proves a fix.
            links = fix_links.values() if isinstance(fix_links, dict) else fix_links
            valid = [link for link in links if link.get("scope") == key and link.get("commit")
                     and link.get("verified_after_fix") is True
                     and link.get("target") == full_composition["target"]]
            negative = any(item.get("category") == "counterexample" for item in scope["observations"])
            if valid and not negative and not scope["cleanup_obligations"]:
                opened.update(status="closed", fix=deepcopy(valid[0]))
                delta["closed"].append(key)
    return records, delta


next_registrations = derive
