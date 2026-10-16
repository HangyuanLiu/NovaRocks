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
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Bind executable paths from Cargo, and reject changed or missing programs."""
import hashlib
import json
import os
import shutil
from pathlib import Path
import sys

REQUIRED = ("novarocks", "novarocks-sql-test", "novarocks-system-tests", "novarocks-error-manifest")

def digest(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()

def record(path):
    path = Path(path).resolve(strict=True)
    if not os.access(path, os.X_OK):
        raise ValueError(f"artifact is not executable: {path}")
    return dict(path=str(path), sha256=digest(path))

try:
    action, source, arg = sys.argv[1:]
    if action == "copy-server":
        found = []
        for line in Path(source).read_text().splitlines():
            v = json.loads(line)
            if v.get("reason") == "compiler-artifact" and v.get("executable") and v["target"]["name"] == "novarocks" and "bin" in v["target"]["kind"] and not v["profile"]["test"]:
                found.append(v["executable"])
        if len(set(found)) != 1:
            raise ValueError("Cargo did not report one actual Server executable")
        shutil.copy2(found[0], arg)
        Path(arg + ".binding.json").write_text(json.dumps(record(arg), indent=2) + "\n")
    elif action == "collect":
        found = {}
        for line in Path(source).read_text().splitlines():
            v = json.loads(line)
            if v.get("reason") != "compiler-artifact" or not v.get("executable"):
                continue
            target = v["target"]
            if "bin" in target["kind"] and not v["profile"]["test"] and target["name"] in REQUIRED:
                found[target["name"]] = record(v["executable"])
        missing = set(REQUIRED) - found.keys()
        if missing:
            raise ValueError("Cargo did not report required binaries: " + ", ".join(sorted(missing)))
        Path(arg).write_text(json.dumps(found, indent=2) + "\n")
    elif action == "verify-copy":
        expected = json.loads(Path(source).read_text())
        if record(arg) != expected:
            raise ValueError("selected variant content changed")
    elif action == "verify":
        expected = json.loads(Path(source).read_text())[arg]
        actual = record(expected["path"])
        if actual != expected:
            raise ValueError("prepared artifact changed; collect paths, build information and hashes again")
        print(actual["path"])
    else:
        raise ValueError("unknown artifact action")
except (OSError, ValueError, KeyError) as error:
    sys.exit("error: " + str(error))
