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
"""Compare saved execution identities without consulting the live checkout."""
import hashlib
import json
import re


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def same_identity(left, right):
    # Missing identities are unknown, including when both sides are missing.
    return left is not None and right is not None and canonical(left) == canonical(right)


def confirmed(record):
    return (isinstance(record, dict) and record.get("inputs_stable") is True
            and same_identity(record.get("inputs_start"), record.get("inputs_end")))


def read_scope_identity(document, native):
    if document.get("schema") != 1 or document.get("domain") not in {"nextest", "doctest"}:
        raise ValueError("unsupported execution identity schema or domain")
    value = document.get("scopes", {}).get(native, {})
    return value.get("inputs_start"), confirmed(value), value.get("declared") is True


def store_only_tool_config(text):
    """Validate G6a's one non-semantic setting without a second TOML parser.

    The producer emits precisely [store] plus one JSON-quoted directory value.
    Any extension is rejected until its execution meaning has been declared.
    """
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) != 2 or lines[0] != "[store]":
        return False
    match = re.fullmatch(r"dir\s*=\s*(\"(?:[^\"\\]|\\.)*\")", lines[1])
    if not match:
        return False
    try:
        return isinstance(json.loads(match.group(1)), str)
    except ValueError:
        return False
