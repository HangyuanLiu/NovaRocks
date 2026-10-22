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

"""Host-shell properties of the guard; container cgroup proof belongs to P08."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_runtime as runtime
from private_deadline import command_guard, guard_service


class PrivateDeadlineTests(unittest.TestCase):
    def context(self, work_remaining=3):
        end = int(time.time()) + work_remaining + 3
        return dict(deadline_unix_ms=end * 1000, cleanup_grace_seconds=3)

    def run_guard(self, command, context):
        argv = command_guard(command, context)
        # macOS has no /proc. This fixture replaces only the Linux clock file;
        # subprocess signals/wait and exact argv still execute on the real OS.
        temporary = tempfile.TemporaryDirectory()
        uptime = Path(temporary.name) / "uptime"
        uptime.write_text("100.00 100.00\n")
        argv[2] = argv[2].replace("/proc/uptime", str(uptime))
        child = subprocess.Popen(argv, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True, text=True)
        try:
            stdout, stderr = child.communicate(timeout=8)
            return child.returncode, stdout, stderr
        finally:
            # Normal communicate already reaped the leader. Only an unreaped
            # original child can still pin the process group for safe cleanup.
            if child.returncode is None:
                try: os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError: pass
                child.wait()
            temporary.cleanup()

    def test_exact_argv_survives_exec_and_shell_metacharacters(self):
        arguments = ["two words", "$(must_not_run)", "a'b"]
        result, output, _ = self.run_guard([sys.executable, "-c", "import sys,json; print(json.dumps(sys.argv[1:]))", *arguments], self.context())
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output), arguments)

    def test_late_start_does_not_start_writer_or_reset_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "writer-started"
            context = dict(deadline_unix_ms=(int(time.time()) - 1) * 1000, cleanup_grace_seconds=3)
            result, _, errors = self.run_guard([sys.executable, "-c", "from pathlib import Path; Path(__import__('sys').argv[1]).touch()", str(marker)], context)
            self.assertEqual(result, 124)
            self.assertIn("RunBudgetExpired", errors)
            self.assertFalse(marker.exists())

    def test_ignored_term_is_killed_before_original_complete_deadline(self):
        context = self.context(work_remaining=2)
        began = time.time()
        result, _, _ = self.run_guard([sys.executable, "-c", "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(30)"], context)
        self.assertEqual(result, 124)
        self.assertLess(time.time(), context["deadline_unix_ms"] / 1000 + 0.25)
        self.assertLess(time.time() - began, 5)

    def test_no_guessed_image_command_and_no_duplicate_template_command(self):
        with self.assertRaisesRegex(runtime.RuntimeFailure, "exact image command"):
            command_guard([], self.context())
        definition = "  spark:\n    image: exact\n    entrypoint: [old]\n    command: [old-command]\n    networks: [iceberg_net]\n"
        guarded = guard_service(definition, ["/bin/actual", "argument"], self.context())
        self.assertEqual(guarded.count("    entrypoint:"), 1)
        self.assertEqual(guarded.count("    command:"), 1)
        self.assertIn("    command: []", guarded)
        self.assertIn("    networks: [iceberg_net]", guarded)

    def test_compose_scalar_preserves_shell_and_original_argv_dollars(self):
        context = self.context()
        original = ["/bin/sh", "-c", "printf '%s' '$unchanged ${literal} $(data)'", "$$"]
        guarded = guard_service("  rest:\n    image: exact\n", original, context)
        encoded = json.loads(next(line.split(": ", 1)[1] for line in guarded.splitlines() if line.startswith("    entrypoint:")))
        self.assertEqual([value.replace("$$", "$") for value in encoded], command_guard(original, context))
        self.assertIn("$${nr_tick%%.*}", encoded[2])
