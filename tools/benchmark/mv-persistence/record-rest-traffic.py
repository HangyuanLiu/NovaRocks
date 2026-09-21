#!/usr/bin/env python3
#
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

"""Record one REST Catalog traffic delta around a synchronous MV refresh."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.request import urlopen


SCALARS = ("requests", "request_body_bytes", "response_body_bytes")
MAPS = ("by_method", "by_status")


def snapshot(uri: str) -> dict:
    with urlopen(uri.rstrip("/") + "/_fixture/catalog-traffic", timeout=5) as response:
        if response.status != 200:
            raise ValueError(f"REST traffic fixture returned HTTP {response.status}")
        value = json.load(response)
    for key in SCALARS:
        if type(value.get(key)) is not int or value[key] < 0:
            raise ValueError(f"invalid REST traffic counter: {key}")
    for key in MAPS:
        if not isinstance(value.get(key), dict) or any(
            type(count) is not int or count < 0 for count in value[key].values()
        ):
            raise ValueError(f"invalid REST traffic counter map: {key}")
    return value


def difference(before: dict, after: dict) -> dict:
    result = {}
    for key in SCALARS:
        result[key] = after[key] - before[key]
        if result[key] < 0:
            raise ValueError(f"REST traffic counter regressed: {key}")
    for key in MAPS:
        result[key] = {
            item: after[key].get(item, 0) - count
            for item, count in before[key].items()
        }
        for item, count in after[key].items():
            if item not in result[key]:
                result[key][item] = count
        if any(count < 0 for count in result[key].values()):
            raise ValueError(f"REST traffic counter map regressed: {key}")
        result[key] = {item: count for item, count in result[key].items() if count}
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--phase", choices=("before", "after"), required=True)
    parser.add_argument("--index", type=int, required=True)
    args = parser.parse_args()
    if args.index < 0:
        parser.error("index must be nonnegative")
    pending = Path(str(args.artifact) + ".pending")
    if args.phase == "before":
        if pending.exists() or (args.index == 0 and args.artifact.exists()):
            parser.error("REST traffic artifact already exists or has an unfinished sample")
        pending.write_text(
            json.dumps({"index": args.index, "before": snapshot(args.uri)}) + "\n",
            encoding="utf-8",
        )
        return
    if not pending.is_file():
        parser.error("REST traffic before snapshot is absent")
    previous = json.loads(pending.read_text(encoding="utf-8"))
    if previous["index"] != args.index:
        parser.error("REST traffic before snapshot belongs to another publication")
    delta = difference(previous["before"], snapshot(args.uri))
    with args.artifact.open("a", encoding="utf-8") as output:
        output.write(json.dumps({"index": args.index, **delta}, sort_keys=True) + "\n")
    pending.unlink()


if __name__ == "__main__":
    main()
