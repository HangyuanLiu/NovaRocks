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
"""Native unit identities and independent evidence dimensions.

These values describe evidence; they never own an executor or modify raw results.
"""
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

KINDS = {"rust-test", "rust-doctest", "sql-case", "system-scenario", "stage"}
MODES = {"default", "loom", "miri"}
RESULTS = {"pass", "fail", "cancelled", "unknown", "not_run"}


@dataclass(frozen=True, order=True)
class UnitKey:
    kind: str
    id: str
    mode: str = "default"

    def __post_init__(self):
        if self.kind not in KINDS or self.mode not in MODES or not self.id:
            raise ValueError("invalid native verification unit")

    def __str__(self):
        return self.kind + ":" + self.id + ("#" + self.mode if self.mode != "default" else "")

    @classmethod
    def parse(cls, value):
        if isinstance(value, cls):
            return value
        if isinstance(value, dict):
            return cls(value["kind"], value["id"], value.get("mode", "default"))
        if not isinstance(value, str) or ":" not in value:
            raise ValueError("unit must contain a native kind and identity")
        kind, native = value.split(":", 1)
        name, marker, mode = native.rpartition("#")
        return cls(kind, name, mode) if marker and mode in MODES else cls(kind, native)


@dataclass
class Scope:
    key: str
    units: Tuple[UnitKey, ...]
    identity: Any = None
    initial_state: Optional[str] = None
    inputs_declared: bool = True
    selected: bool = True
    tier: str = "regular"
    suspended: bool = False
    deferred: Optional[Dict[str, str]] = None

    def __post_init__(self):
        self.units = tuple(UnitKey.parse(unit) for unit in self.units)
        if not self.key or not self.units or len(set(self.units)) != len(self.units):
            raise ValueError("scope requires unique native units")
        if self.initial_state not in (None, "isolated", "reset", "immutable"):
            raise ValueError("unknown initial-state contract")
        if self.tier not in ("regular", "heavy"):
            raise ValueError("unknown verification tier")
        if self.deferred and not all(self.deferred.get(key) for key in ("reason", "backstop")):
            raise ValueError("deferred scope needs reason and backstop")

    @classmethod
    def parse(cls, value):
        return value if isinstance(value, cls) else cls(**value)


@dataclass
class Observation:
    unit: UnitKey
    scope: str
    attempt: str
    lifecycle: str
    identity: Any = None
    result: str = "unknown"
    identity_confirmed: bool = False
    record_complete: bool = False
    terminated: bool = False
    cleanup: str = "unknown"
    applicability: str = "canonical"
    failure: Optional[Dict[str, Any]] = None
    ref: str = ""
    inputs_declared: bool = True
    detail: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.unit = UnitKey.parse(self.unit)
        if self.result not in RESULTS or self.cleanup not in {"ok", "violation", "unknown"}:
            raise ValueError("unknown evidence result or cleanup summary")
        if self.applicability not in {"canonical", "diagnostic", "invalid"}:
            raise ValueError("unknown evidence applicability")

    def to_dict(self):
        value = asdict(self)
        value["unit"] = str(self.unit)
        return value


@dataclass
class Attempt:
    id: str
    observations: List[Observation] = field(default_factory=list)
    formal: bool = False
    current: bool = False
    source: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    root: str = ""

    def __post_init__(self):
        self.observations = [item if isinstance(item, Observation) else Observation(**item)
                             for item in self.observations]
        if any(item.attempt != self.id for item in self.observations):
            raise ValueError("observation belongs to another attempt")
