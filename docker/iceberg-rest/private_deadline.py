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

"""Container-local exit bound for private writers using shared scratch S3."""
from __future__ import annotations
import json
import fixture_runtime as runtime


def command_guard(command, context):
    if (not isinstance(command, list) or not command or
            not all(isinstance(arg, str) and arg and "\0" not in arg for arg in command)):
        raise runtime.RuntimeFailure("RunBudgetRejected", "private writer has no exact image command")
    end = context["deadline_unix_ms"] // 1000
    work = end - context["cleanup_grace_seconds"]
    script = r'''set -eu
for tool in date sleep kill; do
  command -v "$tool" >/dev/null || { echo "RunBudgetRejected: writer deadline capability missing" >&2; exit 75; }
done
nr_now=$(date +%s)
case "$nr_now" in ''|*[!0-9]*) echo "RunBudgetRejected: invalid writer clock" >&2; exit 75;; esac
nr_work=WORK_DEADLINE
nr_end=COMPLETE_DEADLINE
[ "$nr_now" -lt "$nr_work" ] || { echo "RunBudgetExpired: original writer deadline expired" >&2; exit 124; }
# Convert the original wall deadline once. Later wall-clock adjustments may
# not extend the writer's budget. Linux exposes uptime without extra tooling.
read nr_tick nr_unused </proc/uptime || { echo "RunBudgetRejected: monotonic writer clock missing" >&2; exit 75; }
nr_tick=${nr_tick%%.*}
case "$nr_tick" in ''|*[!0-9]*) exit 75;; esac
nr_work_tick=$((nr_tick + nr_work - nr_now))
nr_end_tick=$((nr_tick + nr_end - nr_now))
nr_target=$$
nr_child=
nr_expired=0
nr_interrupted=0
nr_finish() {
  trap '' TERM INT USR1 USR2
  [ -z "$nr_child" ] || kill -KILL "$nr_child" 2>/dev/null || true
  # Keep this shell as namespace PID 1. Its own exit makes the kernel
  # terminate every descendant; same-namespace KILL cannot force PID 1.
  # https://man7.org/linux/man-pages/man7/pid_namespaces.7.html
  exit 124
}
nr_stop() {
  nr_expired=1
  nr_interrupted=1
  [ -z "$nr_child" ] || kill -TERM "$nr_child" 2>/dev/null || true
  (sleep 2; kill -USR2 "$nr_target" 2>/dev/null || true) >/dev/null 2>&1 &
}
trap nr_stop TERM INT USR1
trap nr_finish USR2
"$@" &
nr_child=$!
(
  trap '' TERM INT
  read nr_tick nr_unused </proc/uptime || { kill -USR2 "$nr_target"; exit 1; }
  nr_tick=${nr_tick%%.*}
  [ "$nr_tick" -ge "$nr_work_tick" ] || sleep "$((nr_work_tick - nr_tick))"
  kill -USR1 "$nr_target" 2>/dev/null || exit 0
  read nr_tick nr_unused </proc/uptime || { kill -USR2 "$nr_target"; exit 1; }
  nr_tick=${nr_tick%%.*}
  nr_delay=$((nr_end_tick - nr_tick))
  [ "$nr_delay" -gt 2 ] && nr_delay=2
  [ "$nr_delay" -le 0 ] || sleep "$nr_delay"
  kill -USR2 "$nr_target" 2>/dev/null || true
) >/dev/null 2>&1 &
nr_timer=$!
while :; do
  nr_interrupted=0
  set +e
  wait "$nr_child"
  nr_code=$?
  set -e
  [ "$nr_interrupted" -ne 0 ] || break
done
kill "$nr_timer" 2>/dev/null || true
[ "$nr_expired" -eq 0 ] || exit 124
exit "$nr_code"
'''.replace("WORK_DEADLINE", str(work)).replace("COMPLETE_DEADLINE", str(end))
    return ["/bin/sh", "-c", script, "private-writer-budget", *command]


def guard_service(definition, command, context):
    # The owned templates have single-line entrypoint/command definitions.
    # Preserve every other field, and replace both with one exact image argv.
    lines = [line for line in definition.splitlines(keepends=True)
             if not line.startswith(("    entrypoint:", "    command:"))]
    # Compose interpolates every scalar, including JSON-style argv arrays.
    # Escape both guard variables and literal dollars in the original argv.
    argv = [argument.replace("$", "$$") for argument in command_guard(command, context)]
    return "".join(lines) + "    entrypoint: " + json.dumps(argv) + "\n    command: []\n"
