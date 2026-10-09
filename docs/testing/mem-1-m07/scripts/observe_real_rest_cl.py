#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Bounded stdlib observer for actual normal REST CL; never a provider or injector.

Read-only integration evidence:
* tests/cluster-harness/src/vended_rest_catalog.rs:1162 proxies a real catalog,
  but :1203 injects credentials and its audit lacks CL counters. Do not use its
  response rewriting for this observer.
* tests/system-test-runner/src/scenarios/mv_uea7.rs:49-115 binds catalog URI and
  FE/BE credentials to one IsolatedIcebergRestFixture. Replace only that catalog
  URI with this observer's actual bound URI, preserving the same owner/warehouse
  and static S3 facts. Keep both owners until native cluster shutdown.

CLI: python3 SCRIPT --freeze OBSERVER_BOUND.json --output NEW_DIRECTORY
Control is one JSON line per stdin command, one safe stdout JSON response:
  {"command":"snapshot"}
  {"command":"reset","phase":"catalog_admission"}
  {"command":"stop"}
EOF stops the observer. Reset succeeds only with actual active requests AND
accepted connections both zero, under the same lock used by admission.

Observer freeze schema (exact keys; main must freeze before actual execution):
  schema_version: 1, frozen_before_execution: true,
  observer_sha256: exact SCRIPT digest,
  producer_path: absolute reviewed producer script path,
  producer_sha256: exact producer script digest,
  producer_freeze_path: absolute bound producer JSON,
  producer_freeze_sha256: exact bound JSON digest,
  bounds: BOUNDS below.
The existing producer.validate_freeze verifies private owner/actual manifest/
source binding before any socket bind. No Docker or service lifecycle commands.
OBSERVER_BOUND.json is an endpoint/process receipt, never provider/native READY.

Counters reflect actual requests, complete downstream pages and body sends;
they do not infer FE PID, task identity, SDK positions, or query completion.
metadata_loads/successful_mutations are complete downstream HTTP200 responses,
not a substitute for native typed success. emitted_body_bytes counts bytes
accepted by the observer's client socket, not FE consumption or decoded memory.
Main must save actual FE/catalog config containing this exact observer URI,
native FE PID/start/build, private downstream image/manifest binding, phase
isolation, and observer-process exclusion from FE allocator samples. No producer
or independent verification client may use the observer during native phases.

Semantic headers/status/reason/payload bytes are preserved. HTTP hop framing
(Host, Connection, transfer chunk framing) is adapted for direct close-delimited
connections. No JSON, names, tokens, credentials, errors or status are rewritten.
Observer bound/transport failures close the socket and taint the phase; they
never generate a substitute HTTP error, retry, partial-success or fake catalog.
Compressed listing bodies are observed with bounded gzip decode, then the exact
original compressed bytes/header are forwarded. Unsupported encodings taint
the phase while the actual bounded response is still forwarded unchanged.

--self-test uses only pure parsers and synthetic loopback byte transparency
tests. It is explicitly NOT real provider, native CL or product acceptance.
"""

import argparse
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
import importlib.util
import io
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
from socketserver import TCPServer, ThreadingMixIn
import sys
import threading
import time
import unittest
from urllib.parse import unquote, urlsplit
import zlib


BOUNDS = {
    "max_observers": 256, "max_request_bytes": 65536,
    "max_response_bytes": 16777216, "max_decoded_json_bytes": 16777216,
    "max_header_bytes": 65536, "max_header_fields": 100,
    "request_absolute_deadline_seconds": 30,
}
FREEZE_KEYS = {"schema_version", "frozen_before_execution", "observer_sha256",
               "producer_path", "producer_sha256", "producer_freeze_path",
               "producer_freeze_sha256", "bounds"}
HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
       "te", "trailer", "transfer-encoding", "upgrade"}


class Refusal(Exception):
    """Only static safe reason strings may be passed to this exception."""


def need(condition, reason):
    if not condition:
        raise Refusal(reason)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def parse_json(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            need(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def constant(_):
        raise Refusal("nonfinite JSON value")

    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, UnicodeError, RecursionError):
        raise Refusal("malformed JSON") from None


def read_file(path, cap):
    with Path(path).open("rb") as handle:
        data = handle.read(cap + 1)
    need(len(data) <= cap, "local input exceeds cap")
    return data


def downstream_address(uri):
    value = urlsplit(uri)
    need(value.scheme == "http" and value.hostname and value.port and value.path in ("", "/")
         and value.username is None and value.password is None and not value.query and not value.fragment,
         "invalid exact private downstream URI")
    try:
        address = ipaddress.ip_address(value.hostname)
    except ValueError:
        raise Refusal("numeric downstream address required") from None
    need(address.is_loopback, "private downstream must be loopback")
    return value


def validate_freeze(path):
    raw = read_file(path, 65536)
    freeze = parse_json(raw)
    need(type(freeze) is dict and set(freeze) == FREEZE_KEYS, "invalid observer freeze fields")
    need(type(freeze["schema_version"]) is int and freeze["schema_version"] == 1
         and freeze["frozen_before_execution"] is True, "observer is not frozen")
    need(encode(freeze["bounds"]) == encode(BOUNDS), "observer bounds differ from reviewed draft")
    need(freeze["observer_sha256"] == sha(Path(__file__).read_bytes()), "observer source mismatch")
    for key in ("producer_path", "producer_freeze_path"):
        need(isinstance(freeze[key], str) and Path(freeze[key]).is_absolute(), "explicit absolute producer input required")
    producer_bytes = read_file(freeze["producer_path"], 1048576)
    producer_freeze_bytes = read_file(freeze["producer_freeze_path"], 65536)
    need(sha(producer_bytes) == freeze["producer_sha256"], "producer source digest mismatch")
    need(sha(producer_freeze_bytes) == freeze["producer_freeze_sha256"], "producer freeze digest mismatch")
    spec = importlib.util.spec_from_file_location("reviewed_rest_producer", freeze["producer_path"])
    need(spec is not None and spec.loader is not None, "producer module unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        producer_freeze, producer_sha = module.validate_freeze(freeze["producer_freeze_path"])
    except module.Refusal:
        raise Refusal("actual producer/private-owner binding refused") from None
    need(producer_sha == freeze["producer_freeze_sha256"], "bound producer digest changed")
    binding = producer_freeze["binding"]
    downstream_address(binding["rest_uri"])
    return freeze, sha(raw), binding, producer_freeze["source"]


def header_facts(headers, cap=65536):
    need(len(headers) <= 100, "header field count exceeds cap")
    total = 2
    for name, value in headers:
        need(re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) is not None
             and not any(ch in value for ch in ("\r", "\n", "\x00")), "unsafe header syntax")
        total += len((name + ": " + value + "\r\n").encode("latin-1"))
    need(total <= cap, "header bytes exceed cap")
    lengths = [value for name, value in headers if name.lower() == "content-length"]
    transfers = [value for name, value in headers if name.lower() == "transfer-encoding"]
    need(len(lengths) <= 1 and len(transfers) <= 1 and not (lengths and transfers),
         "ambiguous HTTP body framing")
    if lengths:
        need(re.fullmatch(r"[0-9]+", lengths[0]) is not None, "invalid Content-Length")
    if transfers:
        need(transfers == ["chunked"], "unsupported transfer framing")
    connections = [value for name, value in headers if name.lower() == "connection"]
    hop = set(HOP)
    for connection in connections:
        for token in connection.split(","):
            token = token.strip().lower()
            need(re.fullmatch(r"[!#$%&'*+.^_`|~0-9a-z-]+", token) is not None,
                 "invalid Connection header")
            hop.add(token)
    # Do not silently drop an authorization/entity header declared hop-by-hop.
    need(not ({"authorization", "content-type", "content-encoding", "content-length"} & (hop - HOP)),
         "semantic header declared hop-by-hop")
    return hop, int(lengths[0]) if lengths else None, bool(transfers)


def route(method, target):
    parsed = urlsplit(target)
    need(not parsed.scheme and not parsed.netloc and not parsed.fragment
         and parsed.path.startswith("/v1/"), "request outside actual REST normal scope")
    parts = parsed.path.split("/")
    if method == "GET" and parts == ["", "v1", "namespaces"]:
        return "list_namespaces"
    if len(parts) == 5 and parts[:3] == ["", "v1", "namespaces"] and parts[4] in ("tables", "views"):
        if method == "GET":
            return "list_" + parts[4]
    if len(parts) == 6 and parts[:3] == ["", "v1", "namespaces"] and parts[4] in ("tables", "views"):
        if method == "GET":
            return "load_" + parts[4]
    return "other"


def listing_facts(category, target, body, headers, status, cap=16777216):
    if not category.startswith("list_") or status != 200:
        return {"pages": 0, "names": 0, "name_bytes": 0}
    types = [v for k, v in headers if k.lower() == "content-type"]
    need(len(types) == 1 and types[0].split(";", 1)[0].strip() == "application/json",
         "listing Content-Type is not unique JSON")
    encodings = [v for k, v in headers if k.lower() == "content-encoding"]
    need(len(encodings) <= 1, "duplicate Content-Encoding")
    if encodings and encodings != ["identity"]:
        need(encodings == ["gzip"], "listing encoding cannot be observed")
        try:
            decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
            decoded = decoder.decompress(body, cap + 1)
            need(len(decoded) <= cap and not decoder.unconsumed_tail and decoder.eof
                 and not decoder.unused_data, "gzip JSON exceeds cap or is incomplete")
            body = decoded
        except zlib.error:
            raise Refusal("invalid gzip JSON") from None
    need(len(body) <= cap, "decoded listing JSON exceeds cap")
    value = parse_json(body)
    need(type(value) is dict, "listing response is not object")
    token = value.get("next-page-token")
    need(token is None or (isinstance(token, str) and 0 < len(token.encode()) <= 4096),
         "invalid observed continuation")
    names = []
    if category == "list_namespaces":
        need("namespaces" in value and type(value["namespaces"]) is list, "missing namespaces collection")
        for namespace in value["namespaces"]:
            need(type(namespace) is list and len(namespace) == 1 and isinstance(namespace[0], str),
                 "namespace outside frozen normal shape")
            names.append(namespace[0])
    else:
        need("identifiers" in value and type(value["identifiers"]) is list, "missing identifiers collection")
        namespace = unquote(urlsplit(target).path.split("/")[3], errors="strict")
        for item in value["identifiers"]:
            need(type(item) is dict and set(item) == {"namespace", "name"}
                 and item["namespace"] == [namespace] and isinstance(item["name"], str),
                 "identifier namespace or shape mismatch")
            names.append(item["name"])
    need(len(names) == len(set(names)), "duplicate actual names within page")
    query = urlsplit(target).query
    # Count actual returned short names; namespace-level bytes are counted in
    # namespace-list responses, matching the product listing collector vocabulary.
    return {"pages": 1, "names": len(names), "name_bytes": sum(len(n.encode()) for n in names),
            "returned_names_sha256": sha(encode(names)), "query_sha256": sha(query.encode()),
            "next_token_present": token is not None,
            "next_token_sha256": sha(token.encode()) if token is not None else None}


class Audit:
    def __init__(self, ledger=None):
        self.lock = threading.RLock()
        self.ledger = ledger
        self.connections = 0
        self.active = 0
        self.upstream_active = 0
        self.list_active = 0
        self.sequence = 0
        self.phase = "unclassified"
        self.generation = 1
        self.total_failures = 0
        self.reset_counters()

    def reset_counters(self):
        self.counters = {key: 0 for key in (
            "requests", "upstream_requests_sent", "upstream_mutations_sent", "completed", "observer_connections_peak", "active_peak",
            "upstream_active_peak", "listing_active_peak", "failures", "admission_refusals",
            "pages", "names", "name_bytes", "upstream_body_bytes", "emitted_body_bytes",
            "metadata_loads", "mutations", "successful_mutations")}
        self.routes = {}
        self.statuses = {}

    def event(self, value):
        if self.ledger:
            self.ledger.write(encode(value) + b"\n")
            self.ledger.flush()

    def accept(self):
        with self.lock:
            self.connections += 1
            self.counters["observer_connections_peak"] = max(self.counters["observer_connections_peak"], self.connections)

    def release(self):
        with self.lock:
            self.connections -= 1
            need(self.connections >= 0, "connection ownership underflow")

    def failure(self, reason, admission=False):
        with self.lock:
            self.counters["failures"] += 1
            self.total_failures += 1
            self.counters["admission_refusals"] += int(admission)
            self.event({"event": "failure", "phase": self.phase, "generation": self.generation, "reason": reason})

    def begin(self, method, target, category, peer):
        with self.lock:
            self.sequence += 1
            self.active += 1
            self.counters["requests"] += 1
            self.counters["active_peak"] = max(self.counters["active_peak"], self.active)
            self.counters["mutations"] += int(method in ("POST", "PUT", "PATCH", "DELETE"))
            self.routes[category] = self.routes.get(category, 0) + 1
            return {"sequence": self.sequence, "phase": self.phase, "generation": self.generation,
                    "method": method, "target_sha256": sha(target.encode()), "route": category,
                    "peer_ip": peer[0], "peer_port": peer[1], "status": None,
                    "request_body_bytes": 0, "request_body_sha256": None,
                    "response_body_bytes": 0, "response_body_sha256": None,
                    "emitted_body_bytes": 0, "outcome": "incomplete"}

    def sent(self, method):
        with self.lock:
            self.counters["upstream_requests_sent"] += 1
            self.counters["upstream_mutations_sent"] += int(method in ("POST", "PUT", "PATCH", "DELETE"))

    def upstream_begin(self, category):
        with self.lock:
            self.upstream_active += 1
            self.list_active += int(category.startswith("list_"))
            self.counters["upstream_active_peak"] = max(self.counters["upstream_active_peak"], self.upstream_active)
            self.counters["listing_active_peak"] = max(self.counters["listing_active_peak"], self.list_active)

    def upstream_end(self, category):
        with self.lock:
            self.upstream_active -= 1
            self.list_active -= int(category.startswith("list_"))

    def emitted(self, count):
        with self.lock:
            self.counters["emitted_body_bytes"] += count

    def received(self, record, facts):
        with self.lock:
            self.counters["upstream_body_bytes"] += record["response_body_bytes"]
            self.statuses[str(record["status"])] = self.statuses.get(str(record["status"]), 0) + 1
            for key in ("pages", "names", "name_bytes"):
                self.counters[key] += facts[key]
            if record["status"] == 200:
                self.counters["metadata_loads"] += int(record["route"].startswith("load_"))
                self.counters["successful_mutations"] += int(record["method"] in ("POST", "PUT", "PATCH", "DELETE"))

    def complete(self):
        with self.lock:
            self.counters["completed"] += 1

    def end(self, record):
        with self.lock:
            self.active -= 1
            need(self.active >= 0, "request ownership underflow")
            self.event({"event": "request", **record})

    def snapshot(self):
        with self.lock:
            if self.ledger and not self.ledger.closed:
                self.ledger.flush()
                os.fsync(self.ledger.fileno())
            return {"phase": self.phase, "generation": self.generation,
                    "active_connections": self.connections, "active_requests": self.active,
                    "active_upstream_requests": self.upstream_active,
                    "active_upstream_listings": self.list_active,
                    "valid_observation": self.counters["failures"] == 0,
                    "lifetime_failures": self.total_failures,
                    "counters": dict(self.counters), "routes": dict(self.routes), "statuses": dict(self.statuses),
                    "scope": "actual observer traffic; FE PID and acceptance must be proven externally"}

    def reset(self, phase):
        need(isinstance(phase, str) and re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", phase), "invalid phase label")
        with self.lock:
            need(self.connections == self.active == self.upstream_active == self.list_active == 0,
                 "phase reset requires actual owner idle")
            previous = self.snapshot()
            self.event({"event": "phase-finished", "snapshot": previous})
            self.phase = phase
            self.generation += 1
            self.reset_counters()
            self.event({"event": "phase-started", "phase": phase, "generation": self.generation})
            return {"previous": previous, "current": self.snapshot()}


class HeaderReader:
    def __init__(self, wrapped, cap):
        self.wrapped = wrapped
        self.remaining = cap
        self.header_mode = True

    def readline(self, limit=-1):
        if not self.header_mode:
            return self.wrapped.readline(limit)
        size = self.remaining + 1 if limit < 0 else min(limit, self.remaining + 1)
        raw = self.wrapped.readline(size)
        self.remaining -= len(raw)
        need(self.remaining >= 0, "request line/headers exceed cap")
        return raw

    def read(self, count=-1):
        return self.wrapped.read(count)

    def close(self):
        self.wrapped.close()

    def flush(self):
        self.wrapped.flush()


class BoundedResponse(http.client.HTTPResponse):
    def __init__(self, *args, header_cap, **kwargs):
        super().__init__(*args, **kwargs)
        self.fp = HeaderReader(self.fp, header_cap)

    def begin(self):
        super().begin()
        if self.fp is not None:
            self.fp.header_mode = False

    def _read_and_discard_trailer(self):
        # http.client silently discards trailers. Stock Servlet writes none;
        # detecting an extension is safer than losing a semantic header while
        # claiming transparency. No response is substituted on this refusal.
        line = self.fp.readline(65537)
        need(len(line) <= 65536 and line in (b"", b"\r\n", b"\n"),
             "downstream trailers outside frozen normal contract")


class ObserverServer(ThreadingMixIn, HTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = False
    request_queue_size = 256

    def server_bind(self):
        # HTTPServer.server_bind uses socket.getfqdn even for numeric loopback;
        # no DNS operation is part of this private observer's startup contract.
        TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]

    def __init__(self, address, downstream, audit, bounds=None):
        self.downstream = downstream_address(downstream)
        self.audit = audit
        self.bounds = BOUNDS if bounds is None else bounds
        self.positions = threading.BoundedSemaphore(self.bounds["max_observers"])
        super().__init__(address, ObserverHandler)

    def process_request(self, request, client_address):
        if not self.positions.acquire(blocking=False):
            self.audit.failure("observer connection admission exhausted", admission=True)
            self.shutdown_request(request)
            return
        self.audit.accept()
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.audit.release()
            self.positions.release()
            self.shutdown_request(request)
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.audit.release()
            self.positions.release()

    def handle_error(self, request, client_address):
        # Never print exception bodies, Authorization, query text or payload.
        self.audit.failure("observer handler exception")


class ObserverHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    wbufsize = 0

    def setup(self):
        self.started = time.monotonic()
        self.deadline = self.started + self.server.bounds["request_absolute_deadline_seconds"]
        self.upstream_socket = None
        self.socket_lock = threading.Lock()
        self.connection = self.request
        self.connection.settimeout(self.server.bounds["request_absolute_deadline_seconds"])
        super().setup()
        self.rfile = HeaderReader(self.rfile, self.server.bounds["max_header_bytes"])

        def expire():
            self.server.audit.failure("observer accepted connection absolute deadline expired")
            with self.socket_lock:
                sockets = [self.connection, self.upstream_socket]
                for sock in sockets:
                    if sock is not None:
                        try:
                            sock.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        sock.close()

        self.timer = threading.Timer(max(0, self.deadline - time.monotonic()), expire)
        self.timer.daemon = True
        self.timer.start()

    def finish(self):
        self.timer.cancel()
        self.timer.join()
        super().finish()

    def log_message(self, format_string, *args):
        pass

    def send_error(self, *args, **kwargs):
        self.server.audit.failure("invalid HTTP request; no substitute response")
        self.close_connection = True

    def handle(self):
        # Exactly one RPC per admitted physical connection; no idle keepalive
        # window can survive reset or retain an unclassified future request.
        try:
            self.handle_one_request()
            self.check()
        except (Refusal, OSError, ValueError, http.client.HTTPException):
            self.server.audit.failure("request parse/transport refusal")
        self.close_connection = True

    def check(self):
        need(time.monotonic() < self.deadline, "observer RPC absolute deadline expired")

    def forward(self):
        self.close_connection = True
        audit = self.server.audit
        record = None
        connection = None
        response = None
        upstream_held = False
        category = "other"
        try:
            self.check()
            self.rfile.header_mode = False
            need(len(self.path.encode()) <= self.server.bounds["max_request_bytes"], "request target exceeds cap")
            category = route(self.command, self.path)
            headers = list(self.headers.raw_items())
            hop, length, chunked = header_facts(headers, self.server.bounds["max_header_bytes"])
            need(not chunked, "chunked request outside frozen normal client contract")
            need(not any(k.lower() == "expect" for k, _ in headers), "Expect request outside frozen normal contract")
            hosts = [v for k, v in headers if k.lower() == "host"]
            need(len(hosts) == 1, "request Host is not unique")
            length = 0 if length is None else length
            need(length + len(self.path.encode()) <= self.server.bounds["max_request_bytes"], "request bytes exceed cap")
            record = audit.begin(self.command, self.path, category, self.client_address)
            body = self.rfile.read(length)
            need(len(body) == length, "truncated request body")
            self.check()
            record["request_body_bytes"] = len(body)
            record["request_body_sha256"] = sha(body)
            upstream = self.server.downstream
            connection = http.client.HTTPConnection(upstream.hostname, upstream.port,
                                                    timeout=max(0.001, self.deadline - time.monotonic()))
            connection.response_class = lambda *a, **kw: BoundedResponse(
                *a, header_cap=self.server.bounds["max_header_bytes"], **kw)
            connection.connect()
            with self.socket_lock:
                self.upstream_socket = connection.sock
            self.check()
            audit.upstream_begin(category)
            upstream_held = True
            connection.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
            for name, value in headers:
                if name.lower() not in hop and name.lower() != "host":
                    connection.putheader(name, value)
            connection.putheader("Host", upstream.netloc)
            connection.putheader("Connection", "close")
            connection.endheaders(body)
            record["upstream_request_sent"] = True
            audit.sent(self.command)
            response = connection.getresponse()
            record["status"] = response.status
            if 300 <= response.status < 400:
                # Preserve the actual redirect, but a client following Location
                # could bypass this observer. Such a phase is not an actual
                # complete-traffic receipt for the frozen downstream owner.
                audit.failure("actual redirect creates an observer coverage gap")
            response_headers = list(response.headers.raw_items())
            response_hop, declared, transfer = header_facts(response_headers, self.server.bounds["max_header_bytes"])
            need(response.status >= 200 and response.version in (10, 11)
                 and not any(c in response.reason for c in ("\r", "\n", "\x00")), "unsupported downstream status line")
            if self.command != "HEAD" and declared is not None:
                need(declared <= self.server.bounds["max_response_bytes"], "declared response body exceeds cap")
            raw = response.read(self.server.bounds["max_response_bytes"] + 1)
            self.check()
            need(len(raw) <= self.server.bounds["max_response_bytes"], "response body exceeds cap")
            if self.command != "HEAD" and declared is not None:
                need(len(raw) == declared, "truncated downstream body")
            record["status"] = response.status
            record["response_body_bytes"] = len(raw)
            record["response_body_sha256"] = sha(raw)
            record["semantic_headers_sha256"] = sha(encode([
                (k, v) for k, v in response_headers if k.lower() not in response_hop and k.lower() != "content-length"]))
            audit.upstream_end(category)
            upstream_held = False
            facts = {"pages": 0, "names": 0, "name_bytes": 0}
            try:
                facts = listing_facts(category, self.path, raw, response_headers, response.status,
                                      self.server.bounds["max_decoded_json_bytes"])
            except (Refusal, UnicodeError, ValueError):
                audit.failure("actual listing body cannot be observed; forwarded unchanged")
            record["listing"] = facts
            audit.received(record, facts)
            version = "HTTP/1.0" if response.version == 10 else "HTTP/1.1"
            lines = [f"{version} {response.status} {response.reason}\r\n"]
            for name, value in response_headers:
                if name.lower() not in response_hop and name.lower() != "content-length":
                    lines.append(name + ": " + value + "\r\n")
            # Keep HEAD's actual representation length; otherwise preserve the
            # original Content-Length string or expose exact dechunked body size.
            original_lengths = [v for k, v in response_headers if k.lower() == "content-length"]
            forwarded_length = original_lengths[0] if original_lengths else str(len(raw))
            lines.extend(["Content-Length: " + forwarded_length + "\r\n", "Connection: close\r\n", "\r\n"])
            self.connection.sendall("".join(lines).encode("latin-1"))
            remaining = memoryview(raw)
            while remaining:
                self.check()
                sent = self.connection.send(remaining)
                need(sent > 0, "downstream client closed during body send")
                record["emitted_body_bytes"] += sent
                audit.emitted(sent)
                remaining = remaining[sent:]
            self.check()
            record["outcome"] = "actual-response-forwarded"
            audit.complete()
        except (Refusal, OSError, ValueError, http.client.HTTPException):
            audit.failure("bounded forwarding failed; no substitute response or retry")
        finally:
            if upstream_held:
                audit.upstream_end(category)
            try:
                if response is not None:
                    response.close()
            finally:
                try:
                    if connection is not None:
                        connection.close()
                finally:
                    if record is not None:
                        record["elapsed_ms"] = round((time.monotonic() - self.started) * 1000, 3)
                        audit.end(record)

    do_GET = forward
    do_POST = forward
    do_DELETE = forward
    do_HEAD = forward
    do_PUT = forward
    do_PATCH = forward
    do_OPTIONS = forward

    def handle_expect_100(self):
        self.send_error()
        return False


class PureTests(unittest.TestCase):
    def reject(self, callback):
        with self.assertRaises(Refusal):
            callback()

    def test_parser_rejects_duplicate_missing_cross_scope(self):
        headers = [("Content-Type", "application/json")]
        for body in (b'{"identifiers":[],"identifiers":[]}', b'{}',
                     b'{"identifiers":[{"namespace":["foreign"],"name":"t"}]}',
                     b'{"identifiers":[],"next-page-token":false}'):
            self.reject(lambda b=body: listing_facts("list_tables", "/v1/namespaces/ns/tables", b, headers, 200))
        self.reject(lambda: parse_json(b'{"x":NaN}'))

    def test_header_ambiguity_and_semantic_hop(self):
        for headers in ([('Content-Length', '1'), ('Content-Length', '1')],
                        [('Transfer-Encoding', 'chunked'), ('Content-Length', '1')],
                        [('X-Test', 'x\r\ny')], [('Connection', 'authorization')]):
            self.reject(lambda h=headers: header_facts(h))
        self.reject(lambda: header_facts([('X-Test', 'a' * 65536)]))

    def test_response_header_cap_applies_before_full_header_parse(self):
        class LocalBytesSocket:
            def makefile(self, *args):
                return io.BytesIO(b"HTTP/1.1 200 OK\r\nX-Large: " + b"x" * 1024 + b"\r\n\r\n")

        response = BoundedResponse(LocalBytesSocket(), method="GET", header_cap=64)
        try:
            self.reject(response.begin)
        finally:
            response.close()

    def test_gzip_bound_and_exact_name_bytes(self):
        body = '{"identifiers":[{"namespace":["ns"],"name":"雪"}]}'.encode()
        headers = [("Content-Type", "application/json")]
        facts = listing_facts("list_tables", "/v1/namespaces/ns/tables", body, headers, 200)
        self.assertEqual((facts["pages"], facts["names"], facts["name_bytes"]), (1, 1, 3))
        compressor = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)
        gzip = compressor.compress(body) + compressor.flush()
        compressed_headers = headers + [("Content-Encoding", "gzip")]
        self.assertEqual(listing_facts("list_tables", "/v1/namespaces/ns/tables", gzip, compressed_headers, 200), facts)
        self.reject(lambda: listing_facts("list_tables", "/v1/namespaces/ns/tables", gzip,
                                         compressed_headers, 200, cap=8))

    def test_reset_requires_actual_idle(self):
        audit = Audit()
        audit.accept()
        self.reject(lambda: audit.reset("native_fe"))
        audit.release()
        record = audit.begin("GET", "/v1/config", "other", ("127.0.0.1", 1))
        self.reject(lambda: audit.reset("native_fe"))
        audit.end(record)
        audit.reset("native_fe")
        self.assertEqual(audit.snapshot()["phase"], "native_fe")

    def test_no_proxy_absolute_target_or_foreign_host(self):
        self.reject(lambda: downstream_address("http://rest:8181"))
        self.reject(lambda: downstream_address("http://192.0.2.1:8181"))
        self.reject(lambda: route("GET", "http://other/v1/namespaces"))
        self.reject(lambda: route("GET", "/not-a-catalog"))


class LoopbackTests(unittest.TestCase):
    """Synthetic HTTP byte transparency only, deliberately outside provider gates."""

    def setUp(self):
        self.captured = []
        captured = self.captured
        literal = b'{ "identifiers" : [ {"namespace":["ns"],"name":"literal"} ], "next-page-token":null }\n'
        self.literal = literal

        class LiteralHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def handle_literal(inner):
                length = int(inner.headers.get("Content-Length", "0"))
                captured.append((inner.command, inner.path, inner.rfile.read(length),
                                 inner.headers.get("Authorization"), inner.headers.get_all("X-Exact")))
                status = 409 if "error" in inner.path else 200
                if "slow" in inner.path:
                    time.sleep(0.2)
                inner.send_response_only(status, "Literal reason")
                inner.send_header("Content-Type", "application/json")
                chunked = "chunked" in inner.path
                if chunked:
                    inner.send_header("Transfer-Encoding", "chunked")
                else:
                    inner.send_header("Content-Length", str(len(literal)))
                inner.send_header("X-Downstream", "unchanged")
                inner.send_header("Set-Cookie", "one=1")
                inner.send_header("Set-Cookie", "two=2")
                inner.send_header("Connection", "close")
                inner.end_headers()
                if inner.command != "HEAD":
                    try:
                        if chunked:
                            for start in range(0, len(literal), 7):
                                chunk = literal[start:start + 7]
                                inner.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                            inner.wfile.write(b"0\r\n\r\n")
                        else:
                            inner.wfile.write(literal)
                    except OSError:
                        pass
                inner.close_connection = True

            do_GET = handle_literal
            do_POST = handle_literal
            do_HEAD = handle_literal

            def log_message(inner, *args):
                pass

        class LocalLiteralServer(ThreadingHTTPServer):
            def server_bind(inner):
                TCPServer.server_bind(inner)
                inner.server_name, inner.server_port = inner.server_address[:2]

        self.backend = LocalLiteralServer(("127.0.0.1", 0), LiteralHandler)
        self.backend_thread = threading.Thread(target=self.backend.serve_forever, kwargs={"poll_interval": 0.01})
        self.backend_thread.start()
        self.audit = Audit()
        self.observer = ObserverServer(("127.0.0.1", 0), "http://127.0.0.1:" + str(self.backend.server_port), self.audit)
        self.observer_thread = threading.Thread(target=self.observer.serve_forever, kwargs={"poll_interval": 0.01})
        self.observer_thread.start()

    def tearDown(self):
        self.observer.shutdown()
        self.observer.server_close()
        self.observer_thread.join()
        self.backend.shutdown()
        self.backend.server_close()
        self.backend_thread.join()

    def call(self, method, path, body=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)
        connection.putrequest(method, path, skip_accept_encoding=True)
        connection.putheader("Authorization", "Bearer synthetic-local-secret")
        connection.putheader("X-Exact", "alpha")
        connection.putheader("X-Exact", "beta")
        connection.putheader("Content-Length", str(len(body or b"")))
        connection.endheaders(body)
        response = connection.getresponse()
        result = (response.status, response.reason, list(response.headers.raw_items()), response.read())
        response.close()
        connection.close()
        # The server socket closes before HTTP owner release. Wait for actual
        # idle rather than guessing completion from the client's EOF.
        deadline = time.monotonic() + 2
        while self.audit.snapshot()["active_connections"]:
            need(time.monotonic() < deadline, "local observer did not release connection")
            time.sleep(0.001)
        return result

    def test_literal_body_and_duplicate_semantic_headers(self):
        status, reason, headers, body = self.call("GET", "/v1/namespaces/ns/tables?pageSize=256&pageToken=opaque%2Btoken")
        self.assertEqual((status, reason, body), (200, "Literal reason", self.literal))
        self.assertEqual([v for k, v in headers if k.lower() == "set-cookie"], ["one=1", "two=2"])
        self.assertIn(("X-Downstream", "unchanged"), headers)
        self.assertEqual(self.captured[0][1], "/v1/namespaces/ns/tables?pageSize=256&pageToken=opaque%2Btoken")
        self.assertEqual(self.captured[0][3], "Bearer synthetic-local-secret")
        self.assertEqual(self.captured[0][4], ["alpha", "beta"])
        facts = self.audit.snapshot()
        self.assertTrue(facts["valid_observation"])
        self.assertEqual(facts["counters"]["names"], 1)
        self.assertEqual(facts["counters"]["emitted_body_bytes"], len(self.literal))

    def test_actual_error_and_request_body_are_not_rewritten(self):
        request = b'{ "properties": {"literal":"\\u96ea"} }\n'
        status, reason, headers, body = self.call("POST", "/v1/namespaces/ns/error?literal=%2F", request)
        self.assertEqual((status, reason, body), (409, "Literal reason", self.literal))
        self.assertEqual(self.captured[0][0:3], ("POST", "/v1/namespaces/ns/error?literal=%2F", request))
        snapshot = self.audit.snapshot()
        self.assertEqual(snapshot["counters"]["mutations"], 1)
        self.assertEqual(snapshot["statuses"], {"409": 1})

    def test_head_keeps_actual_representation_length(self):
        status, reason, headers, body = self.call("HEAD", "/v1/namespaces/ns/tables/literal")
        self.assertEqual((status, body), (200, b""))
        self.assertEqual([v for k, v in headers if k.lower() == "content-length"], [str(len(self.literal))])

    def test_chunked_actual_payload_and_semantic_headers_are_preserved(self):
        status, reason, headers, body = self.call("GET", "/v1/namespaces/ns/tables?pageSize=256&chunked=true")
        self.assertEqual((status, reason, body), (200, "Literal reason", self.literal))
        self.assertEqual([v for k, v in headers if k.lower() == "set-cookie"], ["one=1", "two=2"])
        self.assertEqual([v for k, v in headers if k.lower() == "content-length"], [str(len(self.literal))])
        self.assertTrue(self.audit.snapshot()["valid_observation"])

    def restart_observer(self, overrides):
        self.observer.shutdown()
        self.observer.server_close()
        self.observer_thread.join()
        self.observer = ObserverServer(("127.0.0.1", 0), "http://127.0.0.1:" + str(self.backend.server_port),
                                       self.audit, bounds={**BOUNDS, **overrides})
        self.observer_thread = threading.Thread(target=self.observer.serve_forever, kwargs={"poll_interval": 0.01})
        self.observer_thread.start()

    def test_response_cap_closes_without_substitute_status(self):
        self.restart_observer({"max_response_bytes": 8})
        with self.assertRaises(http.client.RemoteDisconnected):
            self.call("GET", "/v1/namespaces/ns/tables")
        self.assertGreater(self.audit.snapshot()["lifetime_failures"], 0)
        self.assertEqual(self.audit.snapshot()["counters"]["completed"], 0)

    def test_absolute_deadline_closes_without_retry(self):
        self.restart_observer({"request_absolute_deadline_seconds": 0.05})
        started = time.monotonic()
        with self.assertRaises(http.client.RemoteDisconnected):
            self.call("GET", "/v1/namespaces/ns/tables/slow")
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(len(self.captured), 1)
        self.assertGreater(self.audit.snapshot()["lifetime_failures"], 0)

    def test_connection_admission_and_reuse(self):
        self.restart_observer({"max_observers": 1})
        first = socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)
        try:
            deadline = time.monotonic() + 2
            while self.audit.snapshot()["active_connections"] != 1:
                need(time.monotonic() < deadline, "local admission not observed")
                time.sleep(0.001)
            self.reject_reset_busy()
            second = socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)
            try:
                self.assertEqual(second.recv(1), b"")
            finally:
                second.close()
            self.assertEqual(self.audit.snapshot()["counters"]["observer_connections_peak"], 1)
            self.assertEqual(self.audit.snapshot()["counters"]["admission_refusals"], 1)
        finally:
            first.shutdown(socket.SHUT_RDWR)
            first.close()
        deadline = time.monotonic() + 2
        while self.audit.snapshot()["active_connections"]:
            need(time.monotonic() < deadline, "local admission not released")
            time.sleep(0.001)
        self.assertEqual(self.call("GET", "/v1/namespaces/ns/tables")[0], 200)

    def reject_reset_busy(self):
        with self.assertRaises(Refusal):
            self.audit.reset("native_fe")


def stdout(value):
    print(encode(value).decode(), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--freeze", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.self_test:
        need(not args.freeze and not args.output, "self-test cannot accept actual service input")
        suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(case)
                                   for case in (PureTests, LoopbackTests))
        passed = unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
        print("Pure parser and synthetic loopback transparency only; no provider/native acceptance.")
        return 0 if passed else 1
    need(args.freeze and args.output, "explicit frozen input and new output directory required")
    freeze, freeze_sha, binding, source = validate_freeze(args.freeze)
    args.output.mkdir(mode=0o700, parents=False, exist_ok=False)
    ledger = (args.output / "observer-audit.jsonl").open("xb")
    audit = Audit(ledger)
    server = ObserverServer(("127.0.0.1", 0), binding["rest_uri"], audit)
    receipt = {"schema_version": 1, "state": "OBSERVER_BOUND", "pid": os.getpid(),
               "started_unix_ns": time.time_ns(), "observer_freeze_sha256": freeze_sha,
               "observer_sha256": freeze["observer_sha256"],
               "observer_uri": "http://127.0.0.1:" + str(server.server_port),
               "downstream_binding": binding, "source": source, "bounds": BOUNDS,
               "scope": "test-only observer process; exclude from FE allocator samples"}
    with (args.output / "OBSERVER_BOUND.json").open("xb") as handle:
        handle.write(encode(receipt) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1})
    thread.start()
    stdout(receipt)
    try:
        while True:
            line = sys.stdin.buffer.readline(65537)
            if not line:
                break
            try:
                need(len(line) <= 65536 and line.endswith(b"\n"), "control input exceeds cap")
                command = parse_json(line)
                need(type(command) is dict, "invalid control command")
                if command == {"command": "snapshot"}:
                    stdout({"command": "snapshot", "audit": audit.snapshot()})
                elif command == {"command": "stop"}:
                    break
                elif set(command) == {"command", "phase"} and command["command"] == "reset":
                    stdout({"command": "reset", **audit.reset(command["phase"])})
                else:
                    raise Refusal("unknown control command")
            except Refusal as error:
                stdout({"control_refused": str(error)})
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
        stdout({"state": "OBSERVER_STOPPED", "audit": audit.snapshot()})
        ledger.close()
    return 0 if audit.snapshot()["lifetime_failures"] == 0 else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (Refusal, OSError, ValueError, KeyError, TypeError):
        # Exceptions may contain private paths/config data: fixed output only.
        print("REFUSED: observer input, local IO or lifecycle failure", file=sys.stderr)
        sys.exit(1)
