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
"""Resolve native scopes and layer membership before reading observations."""
from .model import Scope, UnitKey


def default_scope(unit, declaration=None):
    unit = UnitKey.parse(unit)
    declaration = declaration or {}
    if (unit.kind == "sql-case" and not
            (declaration.get("scope") == "case" and declaration.get("initial_state")
             in {"isolated", "reset", "immutable"})):
        return "sql-suite:" + unit.id.split("/", 1)[0]
    return str(unit)


def required_scopes(layer, declarations):
    if layer not in {"dev", "admission", "full"}:
        raise ValueError("unknown verification layer")
    result = []
    seen = set()
    for value in declarations:
        scope = Scope.parse(value)
        if scope.key in seen:
            raise ValueError("duplicate scope declaration: " + scope.key)
        seen.add(scope.key)
        if scope.suspended:
            continue
        if layer == "admission" and scope.tier != "regular":
            continue
        if layer == "dev" and not scope.selected:
            continue
        result.append(scope)
    return sorted(result, key=lambda scope: scope.key)
