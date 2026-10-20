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
"""Prepare native nextest metadata, or retain one formal Rust execution batch.

This owns executor children and material references. Payload ownership belongs
exclusively to test-support's independent monitor; no PID-ledger cleanup lives here.
Python 3.9 and the standard library are sufficient.
"""
import argparse
import collections
import ctypes
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import queue
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parent / 'lib'))
import identity_snapshot

REPO = Path(__file__).resolve().parents[2]
ARTIFACTS = REPO / "tools/ci/cargo-artifacts.py"
CANCEL = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT)
PAUSE = (signal.SIGTSTP, signal.SIGTTIN, signal.SIGTTOU)
DIAGNOSTIC = tuple(getattr(signal, name) for name in ("SIGUSR1", "SIGINFO") if hasattr(signal, name))
POLL = 0.02


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    with temporary.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
    temporary.replace(path)


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def command_file(root, name, argv, *, cwd=None):
    """Preparation commands preserve raw output and actual exit, including failures."""
    cwd = REPO if cwd is None else cwd
    write_json(root / (name + ".command.json"), dict(argv=list(map(str, argv)), cwd=str(cwd)))
    with (root / (name + ".stdout")).open("xb") as out, (root / (name + ".stderr")).open("xb") as err:
        result = subprocess.run(list(map(str, argv)), cwd=cwd, stdout=out, stderr=err)
    write_json(root / (name + ".exit.json"), dict(returncode=result.returncode))
    if result.returncode:
        raise RuntimeError("{} failed (exit {}); see {}".format(name, result.returncode, root))
    return root / (name + ".stdout")


def native_options(arguments):
    """Keep native selectors; exclude run-only flags from the full-list command.

    Build inputs and metadata are owned by this workspace entry. Narrow selection
    uses nextest's package()/binary()/test()/rdeps() filtersets, never a second graph.
    """
    list_args, run_args = [], list(arguments)
    shared_values = {"-E", "--filterset", "-P", "--profile", "--run-ignored", "--partition",
                     "--platform-filter", "--color", "--show-progress"}
    shared_flags = {"--ignore-default-filter", "--no-pager", "-v", "--verbose"}
    run_values = {"-j", "--test-threads", "--jobs", "--failure-output", "--success-output",
                  "--status-level", "--final-status-level", "--max-progress-running",
                  "--debugger", "--tracer", "--stress-count", "--stress-duration"}
    run_flags = {"--no-capture", "--no-output-indent", "--no-input-handler", "--hide-progress-bar", "--no-fail-fast", "--nff"}
    profile, index = os.environ.get("NEXTEST_PROFILE", "default"), 0
    while index < len(arguments):
        arg = arguments[index]
        if arg == "--":
            list_args += arguments[index:]
            break
        key, equal, value = arg.partition("=")
        if key in shared_values or key in run_values:
            item = [arg]
            if not equal:
                index += 1
                if index >= len(arguments):
                    raise ValueError("missing value for " + key)
                value = arguments[index]
                item.append(value)
            if key in ("--profile", "-P"):
                profile = value
            if key in shared_values:
                list_args += item
        elif key in shared_flags:
            list_args.append(arg)
        elif key in run_flags:
            pass
        elif arg.startswith("-"):
            raise ValueError("unsupported entry option {}; use native runtime filters and the fixed workspace build".format(arg))
        else:
            list_args.append(arg)
        index += 1
    if not profile or "/" in profile or "\\" in profile or profile in (".", ".."):
        raise ValueError("invalid nextest profile")
    return profile, list_args, run_args


def nextest_executable():
    selected = os.environ.get("NOVA_CI_NEXTEST_BIN") or shutil.which("cargo-nextest")
    if not selected:
        raise ValueError("cargo-nextest >=0.9.148 is required; formal installation pins 0.9.148")
    return str(Path(selected).resolve(strict=True))


def prepare(root, arguments, formal_store=None):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=False)
    profile, selection, run_options = native_options(arguments)
    nextest = nextest_executable()
    version = command_file(root, "nextest-version", [nextest, "nextest", "--version"])
    if formal_store is not None and not version.read_text().splitlines()[0].startswith("cargo-nextest 0.9.148 "):
        raise ValueError("formal tool preparation requires the pinned nextest 0.9.148")
    compiler = command_file(root, "rustc-version", ["rustc", "-vV"]).read_text()
    host = next(line.removeprefix("host: ") for line in compiler.splitlines() if line.startswith("host: "))
    shell = 'source "$1/tools/ci/lib/build_capabilities.sh"; REPO_ROOT="$1"; ci_validate_build_inputs dev'
    command_file(root, "build-inputs", ["bash", "-c", shell, "build-inputs", str(REPO)])
    cargo_metadata = command_file(root, "cargo-metadata", ["cargo", "metadata", "--format-version", "1", "--locked"])
    shutil.copyfile(cargo_metadata, root / "cargo-metadata.json")
    config = REPO / ".config/nextest.toml"
    shutil.copyfile(config, root / "nextest.toml")
    config_options = ["--config-file", str(root / "nextest.toml")]
    if formal_store is not None:
        tool_config = root / "tool-config.toml"
        tool_config.write_text("[store]\ndir = " + json.dumps(str(formal_store)) + "\n")
        config_options += ["--tool-config-file", "novarocks:" + str(tool_config)]
    binaries = command_file(root, "binaries-metadata", [nextest, "nextest", "list", "--workspace", "--tests", "--bins",
        "--cargo-profile", "dev", "--locked", "--target-dir", str(REPO / "target"),
        "--list-type", "binaries-only", "--message-format", "json"] + config_options)
    shutil.copyfile(binaries, root / "binaries-metadata.json")
    if Path(json.loads(binaries.read_text())["rust-build-meta"]["target-directory"]).resolve() != (REPO / "target").resolve():
        raise ValueError("native binaries metadata refers to a different target directory")
    shell = ('source "$1/tools/ci/lib/build_capabilities.sh"; REPO_ROOT="$1"; '
             'ci_prepare_main "$2/artifacts.json" && ci_capture_main_builds "$2/artifacts.json" "$2/build-info"')
    command_file(root, "joint-prepare", ["bash", "-c", shell, "joint-prepare", str(REPO), str(root)])
    command_file(root, "bind-tests", [sys.executable, str(ARTIFACTS), "bind-tests", str(root / "binaries-metadata.json"),
        str(root / "artifacts.json.cargo.jsonl"), str(root / "test-artifacts.json"), host])
    reuse = ["--cargo-metadata", str(root / "cargo-metadata.json"), "--binaries-metadata", str(root / "binaries-metadata.json")]
    selected = command_file(root, "selected-tests", [nextest, "nextest", "list"] + reuse + config_options +
        ["--message-format", "json"] + selection)
    shutil.copyfile(selected, root / "selected-tests.json")
    native_list = json.loads(selected.read_text())
    selected_count = sum(case["filter-match"]["status"] == "matches"
        for suite in native_list["rust-suites"].values() for case in suite["testcases"].values())
    if not selected_count:
        raise ValueError("native nextest selection is empty")
    control = ["--no-tests", "fail", "--retries", "0"]
    if not any(value in ("--no-fail-fast", "--nff") for value in run_options):
        control.append("--no-fail-fast")
    if formal_store is not None and not any(value.split("=")[0] in ("-j", "--test-threads", "--jobs") for value in run_options):
        control += ["--test-threads", "4"]
    run_argv = [nextest, "nextest", "run"] + reuse + config_options + control + run_options
    value = dict(schema=1, profile=profile, selected_count=selected_count, run_argv=run_argv,
                 arguments=arguments, nextest_version=version.read_text(), config_sha256=digest(root / "nextest.toml"))
    if formal_store is not None:
        value["tool_config_sha256"] = digest(root / "tool-config.toml")
        value["junit"] = str(formal_store / profile / "junit.xml")
    write_json(root / "prepared.json", value)
    return value


def verify_prepared(root):
    subprocess.run([sys.executable, str(ARTIFACTS), "verify-tests", str(root / "test-artifacts.json")], check=True)
    receipt = json.loads((root / "artifacts.json").read_text())
    for name, value in receipt.items():
        if digest(value["path"]) != value["sha256"]:
            raise ValueError("prepared executable changed: " + name)
    prepared = json.loads((root / "prepared.json").read_text())
    if digest(root / "nextest.toml") != prepared["config_sha256"]:
        raise ValueError("prepared nextest configuration changed")
    if "tool_config_sha256" in prepared and digest(root / "tool-config.toml") != prepared["tool_config_sha256"]:
        raise ValueError("prepared nextest store configuration changed")
    return prepared


def local_filesystem(fd):
    """Match test-support's supported native filesystems without a Python version gate."""
    if sys.platform not in ("darwin", "linux"):
        raise ValueError("formal test runs require native macOS or Linux")
    library = ctypes.CDLL(None, use_errno=True)
    buffer = (ctypes.c_uint64 * 512)()
    if library.fstatfs(fd, ctypes.byref(buffer)) != 0:
        raise OSError(ctypes.get_errno(), "fstatfs")
    raw = bytes(buffer)
    if sys.platform == "darwin":
        valid = int.from_bytes(raw[64:68], sys.byteorder) & 0x1000 and raw[72:88].startswith(b"apfs\0")
    else:
        valid = buffer[0] in (0xEF53, 0x58465342, 0x9123683E, 0x01021994)
    if not valid:
        raise ValueError("formal state requires local APFS/ext/XFS/Btrfs/tmpfs; bind-mount local state into containers")


def open_file(path, flags, mode=0o600):
    fd = os.open(str(path), flags | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0), mode)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise ValueError("expected a regular material file: " + str(path))
    return fd


def create_context(root):
    root.mkdir(mode=0o700)
    descriptor = os.open(str(root), os.O_RDONLY | os.O_CLOEXEC)
    try:
        local_filesystem(descriptor)
    finally:
        os.close(descriptor)
    for name in (".formal", ".gate"):
        os.close(open_file(root / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL))
    pin = open_file(root / ".pin", os.O_RDWR | os.O_CREAT | os.O_EXCL)
    fcntl.flock(pin, fcntl.LOCK_SH)
    return pin


def acquire_until(fd, deadline):
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError("formal cleanup lock deadline expired")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if time.monotonic() >= deadline:
                raise TimeoutError("formal cleanup lock completed after its deadline")
            return
        except OSError as error:
            if error.errno not in (errno.EAGAIN, errno.EACCES):
                raise
        if time.monotonic() >= deadline:
            raise TimeoutError("formal cleanup lock deadline expired")
        time.sleep(POLL)


def cleanup(root, deadline):
    result = dict(schema=1, registration_closed=False, complete=False, records=[], errors=[], late_registrations=[])
    try:
        os.close(open_file(root / "closing", os.O_WRONLY | os.O_CREAT | os.O_EXCL))
        gate = open_file(root / ".gate", os.O_RDWR)
        try:
            acquire_until(gate, deadline)
            os.close(open_file(root / "Closed", os.O_WRONLY | os.O_CREAT | os.O_EXCL))
            result["registration_closed"] = True
        finally:
            os.close(gate)  # Never LOCK_UN a possibly shared open-file description.
        launches = set(root.glob("monitor/*"))
        if any(path.is_symlink() or not path.is_dir() or not (path / "record.jsonl").is_file() for path in launches):
            raise ValueError("registered launch directory is missing its monitor record")
        before = set(root.glob("monitor/*/record.jsonl"))
        for path in sorted(before):
            item = dict(path=str(path.relative_to(root)), complete=False, violations=[])
            result["records"].append(item)
            fd = open_file(path, os.O_RDONLY)
            try:
                acquire_until(fd, deadline)
                caller_error = path.parent / "caller-drop-error.txt"
                try:
                    caller_error_mode = caller_error.lstat().st_mode
                except FileNotFoundError:
                    pass
                else:
                    if not stat.S_ISREG(caller_error_mode):
                        raise ValueError("caller-drop diagnostic is not a regular file: " + str(caller_error.relative_to(root)))
                    item["caller_drop_error"] = str(caller_error.relative_to(root))
                    item["violations"].append("caller Drop reported cleanup failure")
                with os.fdopen(fd, "rb", closefd=False) as stream:
                    raw = stream.read()
                if not raw.endswith(b"\n"):
                    raise ValueError("monitor record has a partial final line")
                events = [json.loads(line) for line in raw.splitlines()]
                if not events or events[0].get("event") != "Pending" or any(e.get("schema") != 1 for e in events):
                    raise ValueError("monitor record lacks schema1 Pending")
                final = events[-1]
                if final.get("event") != "Final" or any(e.get("event") == "CleanupIncomplete" for e in events):
                    raise ValueError("monitor record lacks a complete Final")
                required = ("status_raw", "startup_error", "owner_lost", "errors", "cleanup_complete")
                if not all(key in final for key in required) or final["cleanup_complete"] is not True:
                    raise ValueError("monitor Final lacks actual completed cleanup")
                item["complete"] = True
                item["final"] = final
                item["native"] = events[0].get("native", {})
                item["stdout"] = str((path.parent / events[0]["stdout"]).relative_to(root))
                item["stderr"] = str((path.parent / events[0]["stderr"]).relative_to(root))
                item["violations"].extend(final["errors"])
                if final["startup_error"] is not None:
                    item["violations"].append(final["startup_error"])
                if final["owner_lost"]:
                    item["violations"].append("payload owner was lost")
            except (OSError, ValueError, KeyError, TypeError) as error:
                item["error"] = str(error)
            finally:
                os.close(fd)
        if set(root.glob("monitor/*/record.jsonl")) != before:
            raise ValueError("registered monitor set changed after Closed")
        if time.monotonic() >= deadline:
            raise TimeoutError("formal cleanup summary deadline expired")
        result["late_registrations"] = [p.name for p in sorted(root.glob("late-registration-*"))]
        result["complete"] = all(item["complete"] for item in result["records"])
    except (OSError, ValueError, TypeError) as error:
        result["errors"].append(str(error))
    result["ok"] = (result["registration_closed"] and result["complete"] and not result["errors"] and
                    not result["late_registrations"] and not any(item["violations"] for item in result["records"]))
    write_json(root / "cleanup-summary.json", result)
    return result


class Display:
    """Best-effort display copies never own lifecycle or delay bounded shutdown."""
    def __init__(self, paths):
        self.streams = []
        self.errors = []
        for path, destination in zip(paths, (1, 2)):
            pending = queue.Queue(maxsize=4)
            state = dict(path=path.name, queued_bytes=0, displayed_bytes=0)
            def writer(pending=pending, destination=destination, state=state):
                while True:
                    value = pending.get()
                    try:
                        while value:
                            count = os.write(destination, value)
                            state["displayed_bytes"] += count
                            value = value[count:]
                    except OSError as error:
                        self.errors.append(dict(path=state["path"], error=str(error)))
                        return
            try:
                self.streams.append((path.open("rb"), pending, state))
                threading.Thread(target=writer, daemon=True, name="test-output-display").start()
            except BaseException:
                for stream, _, _ in self.streams:
                    stream.close()
                raise

    def poll(self):
        for stream, pending, state in self.streams:
            if pending.full() or any(e["path"] == state["path"] for e in self.errors):
                continue
            data = stream.read(65536)
            if data:
                pending.put_nowait(data)
                state["queued_bytes"] += len(data)

    def finish(self):
        # One finite catch-up turn; neither growing descendant output nor blocked
        # terminal writes may extend the cleanup deadline or require an EOF.
        try:
            self.poll()
            states = []
            for stream, _, state in self.streams:
                value = state.copy()
                value["raw_bytes"] = os.fstat(stream.fileno()).st_size
                value["complete"] = value["displayed_bytes"] == value["raw_bytes"]
                states.append(value)
            return dict(errors=list(self.errors), streams=states)
        finally:
            for stream, _, _ in self.streams:
                stream.close()


class Executor:
    def __init__(self):
        # Closed inherited stdio must not be reused by record opens while daemon
        # writers address raw FD 1/2. Reserve only absent descriptors, using our
        # own /dev/null descriptions; existing inherited flags remain untouched.
        for number in (0, 1, 2):
            try:
                fcntl.fcntl(number, fcntl.F_GETFD)
            except OSError as error:
                if error.errno != errno.EBADF:
                    raise
                reserved = os.open(os.devnull, os.O_RDWR | os.O_CLOEXEC)
                if reserved != number:
                    os.dup2(reserved, number, inheritable=False)
                    os.close(reserved)
        self.signals = collections.deque()
        self.cancelled = []
        self.deadline = None
        self.previous = {}

    def install(self):
        for number in CANCEL + PAUSE + (signal.SIGCONT,) + DIAGNOSTIC:
            self.previous[number] = signal.signal(number, lambda number, frame: self.signals.append(number))

    def restore(self):
        for number, previous in self.previous.items():
            signal.signal(number, previous)

    def observe_idle(self):
        while self.signals:
            number = self.signals.popleft()
            if number in CANCEL:
                self.cancelled.append(number)
                self.deadline = self.deadline or time.monotonic() + 30

    def execute(self, root, argv, adapter, environment=None):
        root.mkdir(parents=True, exist_ok=True)
        result = dict(started=False, argv=argv, adapter=adapter, cwd=str(REPO), returncode=None,
                      material_root=root.name, stdout="stdout.log", stderr="stderr.log",
                      wait_status=None, signals=[], cancelled=[], errors=[], state_events=[])
        write_json(root / "executor.json", result)
        paths = [root / "stdout.log", root / "stderr.log"]
        outputs = []
        stdin = None
        def high_source(fd):
            if fd >= 3:
                return fd
            try:
                return fcntl.fcntl(fd, fcntl.F_DUPFD_CLOEXEC, 3)
            finally:
                os.close(fd)
        try:
            for path in paths:
                outputs.append(high_source(open_file(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)))
            stdin = high_source(os.open(os.devnull, os.O_RDONLY | os.O_CLOEXEC))
        except BaseException:
            for fd in outputs:
                os.close(fd)
            raise
        actions = [(os.POSIX_SPAWN_DUP2, stdin, 0), (os.POSIX_SPAWN_DUP2, outputs[0], 1),
                   (os.POSIX_SPAWN_DUP2, outputs[1], 2)] + [(os.POSIX_SPAWN_CLOSE, fd) for fd in [stdin] + outputs]
        executable = shutil.which(argv[0]) if not os.path.isabs(argv[0]) else argv[0]
        reset = CANCEL + PAUSE + (signal.SIGCONT, signal.SIGPIPE) + DIAGNOSTIC
        try:
            if not executable:
                raise ValueError("executor is unavailable: " + argv[0])
            pid = os.posix_spawn(executable, argv, environment or os.environ.copy(), file_actions=actions,
                                 setpgroup=0, setsigdef=reset, setsigmask=())
        finally:
            for fd in [stdin] + outputs:
                os.close(fd)
        result.update(started=True, pid=pid)
        display = None
        live = True
        stopped, pause_deadline, killed = False, None, False
        def send(number):
            if not live:
                return
            try:
                if adapter == "nextest":
                    os.kill(pid, number)
                else:
                    os.killpg(pid, number)
            except ProcessLookupError:
                pass  # The unique unreaped Child remains the only addressed identity.
            except OSError as error:
                result["errors"].append("executor signal failed: " + str(error))
        def observed(status):
            nonlocal live
            live = False
            result["wait_status"] = status
            result["returncode"] = os.WEXITSTATUS(status) if os.WIFEXITED(status) else -os.WTERMSIG(status)
            self.deadline = self.deadline or time.monotonic() + 30
        try:
            write_json(root / "executor.json", result)
            display = Display(paths)
            while True:
                display.poll()
                child, status = os.waitpid(pid, os.WNOHANG | os.WUNTRACED | os.WCONTINUED)
                if child:
                    if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                        observed(status)
                        break
                    if os.WIFSTOPPED(status):
                        stopped = True
                        result["state_events"].append(dict(event="stopped", wait_status=status))
                        write_json(root / "executor.json", result)
                        if pause_deadline is not None and not any(number in CANCEL + (signal.SIGCONT,) for number in self.signals):
                            pause_deadline = None
                            os.kill(os.getpid(), signal.SIGSTOP)
                    elif os.WIFCONTINUED(status):
                        stopped = False
                        result["state_events"].append(dict(event="continued", wait_status=status))
                while self.signals:
                    number = self.signals.popleft()
                    result["signals"].append(number)
                    if number in CANCEL:
                        self.cancelled.append(number)
                        result["cancelled"].append(number)
                        self.deadline = self.deadline or time.monotonic() + 30
                        if stopped or pause_deadline is not None:
                            send(signal.SIGCONT)
                            stopped, pause_deadline = False, None
                        send(number)
                    elif number in PAUSE:
                        send(signal.SIGTSTP if adapter == "nextest" else number)
                        pause_deadline = pause_deadline or time.monotonic() + 5
                    elif number == signal.SIGCONT:
                        send(number)
                        stopped, pause_deadline = False, None
                    elif number in DIAGNOSTIC and adapter == "nextest":
                        send(number)
                if pause_deadline is not None and time.monotonic() >= pause_deadline:
                    result["errors"].append("executor did not confirm stopped state within 5s")
                    pause_deadline = None
                    send(signal.SIGCONT)
                    send(signal.SIGTERM)
                    self.deadline = self.deadline or time.monotonic() + 30
                if self.deadline is not None and time.monotonic() >= self.deadline - 1 and not killed:
                    result["errors"].append("executor exceeded cancellation cleanup budget")
                    send(signal.SIGCONT)
                    send(signal.SIGKILL)
                    killed = True
                if self.deadline is not None and time.monotonic() >= self.deadline:
                    result["errors"].append("executor actual exit remains unobserved")
                    break
                time.sleep(POLL)
        except BaseException as error:
            result["primary_error"] = dict(type=type(error).__name__, message=str(error))
            result["errors"].append("post-spawn control failure: " + str(error))
            self.deadline = self.deadline or time.monotonic() + 30
            send(signal.SIGCONT)
            send(signal.SIGTERM)
            while live and time.monotonic() < self.deadline:
                try:
                    child, status = os.waitpid(pid, os.WNOHANG)
                except OSError as wait_error:
                    result["errors"].append("executor wait failed: " + str(wait_error))
                    break
                if child:
                    observed(status)
                    break
                if time.monotonic() >= self.deadline - 1 and not killed:
                    send(signal.SIGKILL)
                    killed = True
                time.sleep(POLL)
            if live:
                result["errors"].append("executor actual exit unobserved after control failure")
        finally:
            while self.signals:
                number = self.signals.popleft()
                result["signals"].append(number)
                if number in CANCEL:
                    self.cancelled.append(number)
                    result["cancelled"].append(number)
            if display is not None:
                try:
                    result["display"] = display.finish()
                except BaseException as error:
                    result["errors"].append("display finalization failed: " + str(error))
            try:
                write_json(root / "executor.json", result)
            except OSError as error:
                result["errors"].append("executor record write failed: " + str(error))
        return result


def reference(root, path, format_name):
    result = dict(path=str(path.relative_to(root)), format=format_name, exists=path.is_file())
    if result["exists"]:
        try:
            with path.open("rb") as stream:
                stream.read(1)
            result["readable"] = True
        except OSError as error:
            result.update(readable=False, error=str(error))
    return result


def execute_domain(executor, root, argv, adapter, environment, before=None):
    """Sealing is independent of executor startup, including pre-spawn failures."""
    pin = create_context(root)
    try:
        try:
            if before is not None:
                before()
            result = executor.execute(root, argv, adapter, environment)
        except BaseException as error:
            result = dict(started=False, returncode=None, argv=argv, adapter=adapter,
                          material_root=root.name, errors=[str(error)],
                          primary_error=dict(type=type(error).__name__, message=str(error)),
                          reason="executor did not start")
        executor.observe_idle()
        deadline = executor.deadline or time.monotonic() + 30
        try:
            collected = cleanup(root, deadline)
        except BaseException as error:
            collected = dict(schema=1, registration_closed=(root / "Closed").is_file(), complete=False,
                             ok=False, records=[], late_registrations=[], errors=[str(error)])
            result["cleanup_errors"] = collected["errors"]
        return result, collected, deadline
    finally:
        os.close(pin)


def confirm_domain_identity(path, record, inputs, environment, domain):
    try:
        scopes = identity_snapshot.rust_snapshot(REPO, inputs, environment, domain)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        record["confirmation_error"] = str(error)
        scopes = {}
    identity_snapshot.confirm(path, record, scopes)


def batch(root, arguments):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    inputs = root / "inputs"
    index = dict(schema=1, complete=False, cwd=str(REPO), argv=sys.argv, arguments=arguments,
                 cargo_profile="dev", stages={}, materials={"inputs": dict(path="inputs", format="native-cargo-and-nextest")})
    write_json(root / "execution-index.json", index)
    executor = Executor()
    executor.install()
    code = 2
    identity_root = root / "identity"
    try:
        # Validation is capability based, including the filesystem actually holding
        # this attempt. No Python release or inherited global environment dump.
        if not hasattr(os, "posix_spawn") or not hasattr(fcntl, "flock"):
            raise ValueError("formal executor requires posix_spawn and flock")
        directory = os.open(str(root), os.O_RDONLY | os.O_CLOEXEC)
        try:
            local_filesystem(directory)
        finally:
            os.close(directory)
        identity_snapshot.atomic_json(identity_root / "source-start.json", identity_snapshot.source_snapshot(REPO))
        git = subprocess.run(["git", "status", "--porcelain"], cwd=REPO, capture_output=True, text=True, check=True)
        index["source"] = dict(commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(), dirty=bool(git.stdout), status=git.stdout)
        preparation = executor.execute(root / "prepare", [sys.executable, str(Path(__file__).resolve()), "prepare",
            "--output", str(inputs), "--formal-store", str(root / "nextest"), "--"] + arguments, "cargo")
        index["stages"]["prepare"] = preparation
        executor.observe_idle()
        if preparation["returncode"] != 0 or executor.cancelled or preparation["errors"]:
            raise RuntimeError("native preparation failed or was cancelled")
        executor.deadline = None
        prepared = verify_prepared(inputs)
        index["nextest_profile"] = prepared["profile"]
        index["nextest_version"] = prepared["nextest_version"]
        index["materials"]["inputs"] = dict(path="inputs", format="native-cargo-and-nextest")
        for key, filename, format_name in (("cargo_metadata", "cargo-metadata.json", "cargo-metadata-v1"),
                ("binaries_metadata", "binaries-metadata.json", "nextest-native-binaries"),
                ("selection", "selected-tests.json", "nextest-native-full-list"),
                ("cargo_artifacts", "artifacts.json.cargo.jsonl", "cargo-compiler-artifact-jsonl"),
                ("program_bindings", "artifacts.json", "novarocks-program-bindings-v1"),
                ("test_bindings", "test-artifacts.json", "novarocks-test-bindings-v1"),
                ("nextest_config", "nextest.toml", "nextest-toml"),
                ("tool_config", "tool-config.toml", "nextest-tool-toml")):
            index["materials"][key] = reference(root, inputs / filename, format_name)
        binaries = json.loads((inputs / "artifacts.json").read_text())
        environment = os.environ.copy()
        environment.update(NOVAROCKS_BIN=binaries["novarocks"]["path"], NOVAROCKS_WORKSPACE_ROOT=str(REPO))
        freshness = executor.execute(root / "error-manifest", [binaries["novarocks-error-manifest"]["path"], "--check"], "cargo", environment)
        index["stages"]["error_manifest"] = freshness
        executor.observe_idle()
        if freshness["returncode"] or freshness["errors"] or executor.cancelled:
            raise RuntimeError("SQL error manifest precondition failed")
        executor.deadline = None
        environment["NOVAROCKS_TEST_RUN_ROOT"] = str(root / "rust-run")
        nextest_identity = identity_snapshot.begin(identity_root / "nextest.json", "nextest",
            identity_snapshot.rust_snapshot(REPO, inputs, environment, "nextest"))
        rust, collected, closing_deadline = execute_domain(executor, root / "rust-run", prepared["run_argv"],
            "nextest", environment, before=lambda: verify_prepared(inputs))
        index["stages"]["nextest"] = rust
        index["materials"]["rust_cleanup"] = reference(root, root / "rust-run/cleanup-summary.json", "novarocks-cleanup-summary-v1")
        junit = Path(prepared["junit"])
        report = reference(root, junit, "junit-xml")
        try:
            if time.monotonic() >= closing_deadline:
                raise TimeoutError("JUnit read deadline expired")
            ET.parse(junit)
            if time.monotonic() >= closing_deadline:
                raise TimeoutError("JUnit read completed after cleanup deadline")
            report["complete"] = True
        except (OSError, ET.ParseError) as error:
            report.update(complete=False, error=str(error))
        index["materials"]["junit"] = report
        confirm_domain_identity(identity_root / "nextest.json", nextest_identity, inputs, environment, "nextest")
        index["materials"]["nextest_identity"] = reference(root, identity_root / "nextest.json", "novarocks-execution-identity-v1")
        write_json(root / "execution-index.json", index)
        executor.observe_idle()
        # Nextest's ordinary assertion-failure exit is 100. Other nonzero exits
        # denote preconditions, execution errors or cancellation and gate doctest.
        if (rust["returncode"] not in (0, 100) or rust["errors"] or executor.cancelled or
                not collected["ok"] or not report["complete"]):
            raise RuntimeError("nextest cancellation, environment, report or cleanup failure gates doctest")
        executor.deadline = None
        environment["NOVAROCKS_TEST_RUN_ROOT"] = str(root / "doctest-run")
        doctest_identity = identity_snapshot.begin(identity_root / "doctest.json", "doctest",
            identity_snapshot.rust_snapshot(REPO, inputs, environment, "doctest"))
        doctest, doc_cleanup, _ = execute_domain(executor, root / "doctest-run",
            ["cargo", "test", "--workspace", "--doc", "--profile", "dev", "--locked", "--target-dir",
             str(REPO / "target"), "--no-fail-fast"], "cargo", environment)
        index["stages"]["doctest"] = doctest
        index["materials"]["doctest_cleanup"] = reference(root, root / "doctest-run/cleanup-summary.json", "novarocks-cleanup-summary-v1")
        confirm_domain_identity(identity_root / "doctest.json", doctest_identity, inputs, environment, "doctest")
        index["materials"]["doctest_identity"] = reference(root, identity_root / "doctest.json", "novarocks-execution-identity-v1")
        write_json(root / "execution-index.json", index)
        executor.observe_idle()
        # Cargo doctests can relink real programs: refresh from a new joint build,
        # never recollect hashes from an older compiler-artifact stream.
        if not executor.cancelled and doc_cleanup["ok"] and not doctest["errors"]:
            executor.deadline = None
            shell = ('source "$1/tools/ci/lib/build_capabilities.sh"; REPO_ROOT="$1"; '
                'ci_prepare_main "$2/artifacts.json" && ci_capture_main_builds "$2/artifacts.json" "$2/build-info-final"')
            refresh = executor.execute(root / "final-prepare", ["bash", "-c", shell, "final-prepare", str(REPO), str(root / "final-prepare")], "cargo")
            index["stages"]["final_prepare"] = refresh
            index["materials"]["final_program_bindings"] = reference(root, root / "final-prepare/artifacts.json", "novarocks-program-bindings-v1")
        else:
            refresh = dict(returncode=None)
        executor.observe_idle()
        index["complete"] = bool(collected["ok"] and doc_cleanup["ok"] and report["complete"] and
            all(stage.get("returncode") is not None and not stage.get("errors") for stage in (rust, doctest, refresh)))
        code = 0 if index["complete"] and not executor.cancelled and all(stage["returncode"] == 0 for stage in (rust, doctest, refresh)) else 1
    except (OSError, ValueError, RuntimeError, KeyError, subprocess.SubprocessError) as error:
        index["error"] = str(error)
    finally:
        executor.observe_idle()
        index["cancelled"] = executor.cancelled
        for name in ("nextest", "doctest", "final_prepare"):
            index["stages"].setdefault(name, dict(started=False, returncode=None, reason=index.get("error", "earlier domain gated this stage")))
        try:
            identity_snapshot.atomic_json(identity_root / "source-end.json", identity_snapshot.source_snapshot(REPO))
        except (OSError, subprocess.SubprocessError) as error:
            index["source_end_error"] = str(error)
        executor.observe_idle()
        index["exit_code"] = 128 + executor.cancelled[0] if executor.cancelled else code
        write_json(root / "execution-index.json", index)
        executor.restore()
    return index["exit_code"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "batch", "exec-argv"))
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--formal-store", type=Path)
    argv = sys.argv[1:]
    separator = argv.index("--") if "--" in argv else len(argv)
    options = parser.parse_args(argv[:separator])
    arguments = argv[separator + 1:]
    if options.action == "prepare":
        prepare(options.output, arguments, options.formal_store)
        return 0
    if options.action == "exec-argv":
        prepared = verify_prepared(options.output)
        os.execv(prepared["run_argv"][0], prepared["run_argv"])
    return batch(options.output, arguments)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, KeyError, subprocess.SubprocessError) as error:
        message = ("error: " + str(error) + "\n").encode()
        def diagnostic(message=message):
            try:
                os.write(2, message)
            except OSError:
                pass
        writer = threading.Thread(target=diagnostic, daemon=True, name="test-run-error-display")
        writer.start()
        writer.join(0.02)
        sys.exit(2)
