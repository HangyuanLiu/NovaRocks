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
"""Strict, expiring dispositions apply only to their native unit and failure."""
import datetime
import json
from pathlib import Path

from .model import UnitKey


def _date(value, label):
    try:
        parsed = datetime.date.fromisoformat(value)
    except (TypeError, ValueError):
        raise ValueError(label + " must be an ISO date") from None
    if parsed.isoformat() != value:
        raise ValueError(label + " must be an ISO date")
    return parsed


class Dispositions:
    def __init__(self, document=None, today=None):
        document = document or {"schema": 1, "known_failures": [], "quarantine": []}
        if document.get("schema") != 1:
            raise ValueError("unsupported dispositions schema")
        self.today = _date(today, "today") if isinstance(today, str) else today or datetime.date.today()
        self.known_failures = []
        self.quarantine = {}
        for entry in document.get("known_failures", []):
            self._required(entry, ("reason", "owner", "expires"))
            unit = UnitKey.parse(entry.get("unit"))
            failure = entry.get("failure", {})
            if unit.kind != "sql-case" or unit.mode != "default":
                raise ValueError("known failures support default SQL cases only")
            if failure.get("class") == "error_code":
                if set(failure) != {"class", "code"} or not isinstance(failure["code"], str) or not failure["code"]:
                    raise ValueError("error-code failure needs an exact code")
            elif failure.get("class") == "mismatch":
                if (set(failure) != {"class", "statement", "actual_digest"}
                        or type(failure["statement"]) is not int or failure["statement"] < 0
                        or not isinstance(failure["actual_digest"], str) or not failure["actual_digest"]):
                    raise ValueError("mismatch failure needs statement and actual digest")
            else:
                raise ValueError("unsupported known-failure class")
            expires = _date(entry["expires"], "expires")
            self.known_failures.append((unit, failure, expires, dict(entry)))
        for entry in document.get("quarantine", []):
            self._required(entry, ("reason", "owner", "exit_condition", "review_by"))
            unit = UnitKey.parse(entry.get("unit"))
            if unit in self.quarantine:
                raise ValueError("duplicate quarantine unit")
            self.quarantine[unit] = (_date(entry["review_by"], "review_by"), dict(entry))

    @staticmethod
    def _required(entry, keys):
        if not isinstance(entry, dict) or not all(isinstance(entry.get(key), str) and entry[key].strip() for key in keys):
            raise ValueError("disposition requires " + ", ".join(keys))

    @classmethod
    def from_dict(cls, document, today=None):
        return cls(document, today)

    @classmethod
    def read(cls, path, today=None):
        return cls(json.loads(Path(path).read_text()), today)

    def match(self, observation):
        if observation.result != "fail" or not observation.failure:
            return None
        reason = observation.detail.get("reason") or observation.detail.get("raw", {}).get("reason")
        if reason in {"suite_init_failed", "load_failed", "aborted"}:
            return None
        failure = observation.failure
        for unit, expected, expires, entry in self.known_failures:
            if unit != observation.unit or expires < self.today:
                continue
            if expected["class"] == "error_code":
                matched = failure.get("class") == "error_code" and failure.get("error_code", failure.get("code")) == expected["code"]
            else:
                matched = (failure.get("class") == "mismatch"
                           and failure.get("statement_index", failure.get("statement")) == expected["statement"]
                           and failure.get("actual_digest") == expected["actual_digest"])
            if matched:
                return entry
        return None

    def excluded(self, unit):
        value = self.quarantine.get(UnitKey.parse(unit))
        return value[1] if value and value[0] >= self.today else None

    def stale(self, observations):
        passed = {item.unit for item in observations if item.result == "pass"}
        return [entry for unit, _, _, entry in self.known_failures if unit in passed]
