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
import datetime as dt
import hashlib
import hmac
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


SCALARS = (
    "requests",
    "request_body_bytes",
    "response_body_bytes",
    "table_commit_requests",
    "table_commit_roundtrip_nanos",
)
MAPS = ("by_method", "by_status")


def s3_marker(uri: str, artifact: Path, index: int, phase: str) -> str:
    """Issue one signed, read-only HEAD whose unique path marks the S3 trace."""
    endpoint = urlsplit(uri)
    if endpoint.scheme != "http" or not endpoint.netloc or endpoint.path not in ("", "/"):
        raise ValueError("S3 trace marker requires the isolated HTTP MinIO endpoint")
    access_key = os.environ["AWS_S3_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_S3_SECRET_ACCESS_KEY"]
    identity = hashlib.sha256(str(artifact.resolve()).encode()).hexdigest()[:16]
    token = f"uea7-marker-{identity}-{index}-{phase}"
    path = f"/warehouse/_novarocks/trace-markers/{token}"
    now = dt.datetime.now(dt.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    empty_hash = hashlib.sha256(b"").hexdigest()
    headers = (
        f"host:{endpoint.netloc}\n"
        f"x-amz-content-sha256:{empty_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"HEAD\n{path}\n\n{headers}\n{signed_headers}\n{empty_hash}"
    scope = f"{date}/us-east-1/s3/aws4_request"
    to_sign = (
        f"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n"
        f"{hashlib.sha256(canonical.encode()).hexdigest()}"
    )
    key = ("AWS4" + secret_key).encode()
    for part in (date, "us-east-1", "s3", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    request = Request(
        uri.rstrip("/") + path,
        method="HEAD",
        headers={
            "Host": endpoint.netloc,
            "X-Amz-Content-Sha256": empty_hash,
            "X-Amz-Date": amz_date,
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={access_key}/{scope},"
                f"SignedHeaders={signed_headers},Signature={signature}"
            ),
        },
    )
    try:
        with urlopen(request, timeout=5) as response:
            raise ValueError(f"S3 trace marker unexpectedly exists: HTTP {response.status}")
    except HTTPError as error:
        if error.code != 404:
            raise ValueError(f"S3 trace marker failed: HTTP {error.code}") from error
    return token


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
    parser.add_argument("--s3-marker-uri")
    parser.add_argument("--phase", choices=("before", "after"), required=True)
    parser.add_argument("--index", type=int, required=True)
    args = parser.parse_args()
    if args.index < 0:
        parser.error("index must be nonnegative")
    pending = Path(str(args.artifact) + ".pending")
    if args.phase == "before":
        if pending.exists() or (args.index == 0 and args.artifact.exists()):
            parser.error("REST traffic artifact already exists or has an unfinished sample")
        before = snapshot(args.uri)
        marker = (
            s3_marker(args.s3_marker_uri, args.artifact, args.index, args.phase)
            if args.s3_marker_uri
            else None
        )
        start_unix_ns = time.time_ns()
        pending.write_text(
            json.dumps(
                {
                    "index": args.index,
                    "before": before,
                    "start_unix_ns": start_unix_ns,
                    "s3_start_marker": marker,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        return
    if not pending.is_file():
        parser.error("REST traffic before snapshot is absent")
    previous = json.loads(pending.read_text(encoding="utf-8"))
    if previous["index"] != args.index:
        parser.error("REST traffic before snapshot belongs to another publication")
    if bool(previous.get("s3_start_marker")) != bool(args.s3_marker_uri):
        parser.error("S3 trace marker mode changed within a publication")
    marker = (
        s3_marker(args.s3_marker_uri, args.artifact, args.index, args.phase)
        if args.s3_marker_uri
        else None
    )
    end_unix_ns = time.time_ns()
    delta = difference(previous["before"], snapshot(args.uri))
    if delta["table_commit_requests"] != 1:
        parser.error("publication window did not contain exactly one target table commit")
    if end_unix_ns <= previous["start_unix_ns"]:
        parser.error("publication traffic window has nonincreasing wall time")
    with args.artifact.open("a", encoding="utf-8") as output:
        output.write(
            json.dumps(
                {
                    "index": args.index,
                    "start_unix_ns": previous["start_unix_ns"],
                    "end_unix_ns": end_unix_ns,
                    "s3_start_marker": previous.get("s3_start_marker"),
                    "s3_end_marker": marker,
                    **delta,
                },
                sort_keys=True,
            )
            + "\n"
        )
    pending.unlink()


if __name__ == "__main__":
    main()
