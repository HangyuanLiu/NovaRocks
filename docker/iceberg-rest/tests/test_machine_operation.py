# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Real command-supervisor/CLI processes against a hermetic fake Docker.

Only temporary files represent containers. Nothing invokes the real Docker
binary, socket, network, image store, or daemon.
"""
import errno
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
import fixture_runtime as runtime
import machine_runtime as machine


FAKE_DOCKER = r'''#!/usr/bin/env python3
import json,os,pathlib,signal,subprocess,sys,time
root=pathlib.Path(__file__).resolve().parent.parent
args=sys.argv[1:]
config=json.loads((root/'fake-config.json').read_text())
state=root/'container.json'
def event(name, **facts):
    value=dict(event=name,pid=os.getpid(),ppid=os.getppid(),args=args,**facts)
    with (root/'events.jsonl').open('a') as out:
        out.write(json.dumps(value)+'\n');out.flush();os.fsync(out.fileno())
def gate(name):
    while not (root/name).exists():time.sleep(0.01)
def save(value):
    path=root/('container-'+str(os.getpid())+'.tmp')
    path.write_text(json.dumps(value));path.replace(state)
if args[0]=='_plugin':
    event('plugin.entered',group=os.getpgrp())
    signal.signal(signal.SIGTERM,signal.SIG_IGN if config.get('plugin_ignore_term') else lambda *_:event('plugin.term'))
    while not (root/'plugin-release').exists():
        with (root/'plugin-writes').open('a') as out:out.write('x')
        time.sleep(0.01)
    (root/'plugin-side-effect').write_text('finished')
    event('plugin.exit')
    raise SystemExit(0)
if config.get('operation')=='control' and args==config.get('control_args',['volume','create','fixture-test-volume']):
    event('control.entered')
    if config.get('hold_control'):gate('control-release')
    if config.get('plugin'):
        plugin=subprocess.Popen([sys.executable,__file__,'_plugin'],close_fds=True)
        event('control.plugin',plugin_pid=plugin.pid)
    code=config.get('control_exit',0)
    if code==0:
        (root/'control-side-effect').write_text('finished')
        print('fake control output',flush=True)
    else:
        print(config.get('control_stderr','phase-specific control failure'),file=sys.stderr,flush=True)
    event('control.exit',code=code)
    raise SystemExit(code)
if args[:2]==['context','show']:
    event('context.show');print('fixture-fake-context');raise SystemExit(0)
if args[:2]==['context','inspect']:
    event('context.inspect');print(json.dumps('unix:///fixture-fake-docker.sock'));raise SystemExit(0)
if args and args[0]=='info':
    event('daemon.info');print('fake-daemon');raise SystemExit(0)
if args[0]=='_late-create':
    event('late.entered')
    gate('late-release')
    save(json.loads((root/'late-container.json').read_text()))
    event('late.created')
    raise SystemExit(0)
if args[0]=='create':
    event('create.entered')
    name=args[args.index('--name')+1]
    labels={}
    for i,a in enumerate(args):
        if a=='--label':
            key,value=args[i+1].split('=',1);labels[key]=value
    image=args[args.index('--entrypoint')+2]
    info=dict(Id='container-exact-42',Name=name,Image=image,
              Config=dict(Labels=labels),State=dict(Running=False))
    if config.get('foreign'):
        info['Image']='sha256:foreign-image'
    if config.get('hold_create'):gate('create-release')
    if config.get('unknown_create'):
        if config.get('late_create'):
            (root/'late-container.json').write_text(json.dumps(info))
            subprocess.Popen([sys.executable,__file__,'_late-create'],
                             stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL,start_new_session=True,close_fds=True)
        print('phase-specific create failure',file=sys.stderr,flush=True)
        event('create.exit',code=1)
        raise SystemExit(1)
    save(info)
    print(info['Id'],flush=True)
    event('create.exit',code=0)
    raise SystemExit(0)
if args[:2]==['container','inspect']:
    event('inspect')
    if state.exists():
        info=json.loads(state.read_text())
        if args[2] in (info['Id'],info['Name']):
            print(json.dumps(info));raise SystemExit(0)
    print('Error: No such container: '+args[2],file=sys.stderr)
    raise SystemExit(1)
if args[0]=='start':
    info=json.loads(state.read_text())
    assert args==['start','--attach',info['Id']],args
    info['State']['Running']=True;save(info)
    event('start.entered')
    if config.get('hold_start'):
        def terminate(signum,frame):
            event('start.signal',signal=signum)
            gate('start-release')
            event('start.exit',code=128+signum)
            raise SystemExit(128+signum)
        signal.signal(signal.SIGTERM,terminate)
        while True:time.sleep(0.01)
    if config.get('binary'):
        sys.stdout.buffer.write(b'\x00\xff\x80PAR1\n');sys.stdout.buffer.flush()
    else:
        print('fake mc output',flush=True)
    event('start.exit',code=0)
    raise SystemExit(0)
if args[:2]==['container','rm']:
    event('rm.entered')
    if config.get('hold_rm'):gate('rm-release')
    info=json.loads(state.read_text())
    assert args==['container','rm','--force',info['Id']],args
    state.unlink();event('rm.exit')
    raise SystemExit(0)
event('unexpected')
raise SystemExit(91)
'''


def backend_for(root):
    return machine.MachineDocker(timeout=8, environment={**os.environ,
        "PATH": str(root / "bin") + os.pathsep + os.environ.get("PATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1"})


def owner_for(root):
    return machine.MachineOwner(root / "state", daemon="fake-daemon",
                                backend=backend_for(root), timeout=8)


def operation_record(root, owner):
    directory = owner.base / "activations" / "generation-1"
    directory.mkdir(parents=True, exist_ok=True)
    return dict(kind="machine", namespace=owner.machine_id, control_root=str(owner.root),
                version="test-version", generation="generation-1", network="fake-machine-network",
                compose_file=str(directory / "compose.yml"),
                credentials=dict(access_key="fixture-test-user", secret_key="fixture-test-secret"),
                images=dict(mc=dict(image_id="sha256:fixture-test-image")))


def run_owner(root):
    owner = owner_for(root)
    record = operation_record(root, owner)
    options = json.loads((root / "fake-config.json").read_text())
    owner.backend.timeout = options.get("timeout", 8)
    if options.get("saved_maintenance"):
        runtime.atomic_json(owner.base / "state.json", {**record, "schema":machine.PROTOCOL,
            "daemon_id":owner.daemon_id, "phase":"maintenance"})
    if options.get("supervisor_start_delay"):
        original = owner.backend._supervise
        def delayed(*args, **kwargs):
            time.sleep(options["supervisor_start_delay"])
            return original(*args, **kwargs)
        owner.backend._supervise = delayed
    result = {}
    try:
        with owner.maintenance_lock():
            (root / "owner-locked").write_text(str(owner.base / "maintenance.lock"))
            if options.get("operation") == "control":
                output = owner.backend.command(options.get("control_args", ["volume", "create", "fixture-test-volume"]))
            else:
                output = owner.backend.run_mc(record, "echo fixture-command", binary=options.get("binary", False))
            if isinstance(output, bytes):
                result["stdout_hex"] = output.hex()
            else:
                result["stdout"] = output
        result["exit"] = 0
    except BaseException as error:
        result.update(exit=1, error=getattr(error, "code", type(error).__name__))
    (root / "owner-result.json").write_text(json.dumps(result))
    return result["exit"]


class MachineOperationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="novarocks-machine-operation-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "bin").mkdir()
        cli = self.root / "bin" / "docker"
        cli.write_text(FAKE_DOCKER)
        cli.chmod(0o700)
        self.owner_process = None
        self.addCleanup(self.finish)

    def configure(self, **options):
        (self.root / "fake-config.json").write_text(json.dumps(options))

    def start_owner(self, **options):
        self.configure(**options)
        log = (self.root / "owner.log").open("wb")
        try:
            self.owner_process = subprocess.Popen([sys.executable, str(HERE), "--fixture-owner", str(self.root)],
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        finally:
            log.close()
        return self.owner_process

    def until(self, predicate, label, timeout=6):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.01)
        self.fail("timed out waiting for " + label + "; events=" + json.dumps(self.events()))

    def events(self):
        path = self.root / "events.jsonl"
        if not path.exists():
            return []
        values = []
        for line in path.read_text().splitlines():
            try:
                values.append(json.loads(line))
            except ValueError:
                pass  # A concurrently appended last line is not yet committed.
        return values

    def event(self, name):
        return next((e for e in self.events() if e["event"] == name), None)

    def journal_path(self):
        paths = list((self.root / "state").glob("*/activations/**/operations/*.json"))
        return paths[0] if len(paths) == 1 else None

    def fact(self):
        path = self.journal_path()
        return json.loads(path.read_text()) if path else {}

    def control_fact(self):
        paths = list((self.root / "state").glob("*/operations/control-*.json"))
        self.assertEqual(len(paths), 1)
        return json.loads(paths[0].read_text())

    def lock_held(self):
        marker = self.root / "owner-locked"
        if not marker.exists():
            return False
        with Path(marker.read_text()).open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(lock, fcntl.LOCK_UN)
            return False

    def release(self, name):
        (self.root / name).touch()

    def wait_owner(self, expected):
        self.assertEqual(self.owner_process.wait(timeout=10), expected,
                         (self.root / "owner.log").read_text())
        return json.loads((self.root / "owner-result.json").read_text())

    def finish(self):
        # Every gate belongs to this test's fake CLI; release before reaping.
        for name in ("create-release", "start-release", "rm-release", "late-release", "control-release", "plugin-release"):
            self.release(name)
        if self.owner_process is not None and self.owner_process.poll() is None:
            self.owner_process.kill()
            self.owner_process.wait(timeout=5)
        deadline = time.monotonic() + 8
        while self.lock_held() and time.monotonic() < deadline:
            time.sleep(0.01)
        late = self.event("late.entered")
        if late:
            deadline = time.monotonic() + 3
            while not self.event("late.created") and time.monotonic() < deadline:
                time.sleep(0.01)
        evidence = os.environ.get("NOVA_TEST_MONITOR_EVIDENCE")
        if evidence:
            destination = Path(evidence) / self.id().rsplit(".", 1)[-1]
            shutil.copytree(self.root, destination, dirs_exist_ok=True)

    def test_normal_create_start_and_cleanup_use_exact_container_identity(self):
        self.start_owner()
        result = self.wait_owner(0)
        self.assertEqual(result["stdout"], "fake mc output\n")
        events = self.events()
        starts = [e for e in events if e["event"] == "start.entered"]
        removes = [e for e in events if e["event"] == "rm.entered"]
        self.assertEqual(len(starts), 1)
        self.assertEqual(len(removes), 1)
        self.assertEqual(starts[0]["args"], ["start", "--attach", "container-exact-42"])
        self.assertEqual(removes[0]["args"], ["container", "rm", "--force", "container-exact-42"])
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertTrue(self.fact()["container_absent"])
        self.assertFalse((self.root / "container.json").exists())
        self.assertFalse(self.lock_held())
        self.assertNotIn("fixture-test-secret", json.dumps(events))
        self.assertEqual(self.journal_path().with_suffix(".env").stat().st_mode & 0o777, 0o600)

    def test_sigkill_during_create_retains_inherited_flock_through_actual_cleanup(self):
        self.start_owner(hold_create=True, hold_rm=True)
        self.until(lambda: self.event("create.entered"), "create CLI")
        self.owner_process.kill()
        self.assertEqual(self.owner_process.wait(timeout=3), -signal.SIGKILL)
        self.assertTrue(self.lock_held(), "monitor must retain the owner's open flock description")
        self.assertIsNone(self.event("create.exit"))
        self.release("create-release")
        self.until(lambda: self.event("rm.entered"), "exact container cleanup")
        self.assertTrue(self.lock_held(), "flock must cover the live cleanup CLI and container")
        self.assertTrue((self.root / "container.json").exists())
        self.assertIsNone(self.event("start.entered"), "late create must never dispatch start")
        self.release("rm-release")
        self.until(lambda: not self.lock_held(), "monitor's final lock release")
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertTrue(self.fact()["interrupted"])
        self.assertFalse((self.root / "container.json").exists())
        self.assertIsNotNone(self.event("create.exit"))
        self.assertIsNotNone(self.event("rm.exit"))

    def test_sigkill_during_attach_joins_cli_before_removing_container(self):
        self.start_owner(hold_start=True, hold_rm=True)
        self.until(lambda: self.event("start.entered"), "attached Docker CLI")
        self.owner_process.kill()
        self.owner_process.wait(timeout=3)
        self.until(lambda: self.event("start.signal"), "monitor cancellation signal")
        self.assertTrue(self.lock_held())
        self.assertIsNone(self.event("rm.entered"), "container cleanup cannot overtake CLI exit")
        self.release("start-release")
        self.until(lambda: self.event("rm.entered"), "cleanup after CLI wait")
        names = [e["event"] for e in self.events()]
        self.assertLess(names.index("start.exit"), names.index("rm.entered"))
        self.assertTrue(self.lock_held())
        self.release("rm-release")
        self.until(lambda: not self.lock_held(), "settled cleanup")
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertFalse(self.fact()["command_succeeded"])

    def test_unknown_create_plus_observed_absence_never_settles(self):
        self.start_owner(unknown_create=True)
        result = self.wait_owner(1)
        self.assertEqual(result["error"], "ContainerCleanupUnconfirmed")
        self.assertEqual(self.fact()["phase"], "create-unknown")
        self.assertNotIn("container_absent", self.fact())
        owner = owner_for(self.root)
        with owner.maintenance_lock():
            with self.assertRaises(runtime.RuntimeFailure) as caught:
                owner.backend.settle_operations(owner.base)
        self.assertEqual(caught.exception.code, "ContainerCleanupUnconfirmed")
        self.assertEqual(self.fact()["phase"], "create-unknown")
        self.assertIsNone(self.event("start.entered"))
        streams = list(self.journal_path().parent.glob("*.stderr"))
        self.assertTrue(any(b"phase-specific create failure" in p.read_bytes() for p in streams),
                        "inspect/cleanup must not overwrite create stderr")

    def test_binary_mc_output_preserves_non_utf8_bytes(self):
        self.start_owner(binary=True)
        result = self.wait_owner(0)
        self.assertEqual(bytes.fromhex(result["stdout_hex"]), b"\x00\xff\x80PAR1\n")
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertFalse((self.root / "container.json").exists())

    def test_control_sigkill_keeps_flock_until_current_cli_actually_exits(self):
        self.start_owner(operation="control", hold_control=True)
        self.until(lambda: self.event("control.entered"), "machine control CLI")
        self.owner_process.kill()
        self.assertEqual(self.owner_process.wait(timeout=3), -signal.SIGKILL)
        self.assertTrue(self.lock_held())
        self.assertFalse((self.root / "control-side-effect").exists())
        self.assertIsNone(self.event("control.exit"))
        self.release("control-release")
        self.until(lambda: not self.lock_held(), "control CLI exit and inherited flock release")
        self.assertIsNotNone(self.event("control.exit"))
        self.assertTrue((self.root / "control-side-effect").exists())
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "settled")
        self.assertTrue(fact["interrupted"])
        self.assertTrue(fact["command_succeeded"], "the one already-dispatched command completed")
        self.assertEqual(sum(e["event"] == "control.entered" for e in self.events()), 1)

    def test_mutating_control_timeout_remains_unknown_and_blocks_next_activation(self):
        self.start_owner(operation="control", hold_control=True, timeout=0.5)
        self.wait_owner(1)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "command-unknown")
        self.assertTrue(fact["timed_out"])
        self.assertTrue(fact["mutating"])
        self.assertFalse(self.lock_held())
        owner = owner_for(self.root)
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            owner.activate({})
        self.assertEqual(caught.exception.code, "MachineOperationUnconfirmed")
        self.assertEqual(self.control_fact()["phase"], "command-unknown")
        self.assertEqual(sum(e["event"] == "control.entered" for e in self.events()), 1)
        self.assertFalse((self.root / "control-side-effect").exists())

    def test_control_leader_stays_unreaped_until_plugin_actually_exits(self):
        self.start_owner(operation="control", plugin=True)
        entered = self.until(lambda: self.event("plugin.entered"), "plugin descendant")
        leader = self.until(lambda: self.event("control.exit"), "CLI leader exit")
        self.assertEqual(entered["group"], leader["pid"])
        self.assertTrue(self.lock_held())
        self.assertIsNone(self.owner_process.poll())
        state = subprocess.check_output(["/bin/ps", "-o", "stat=", "-p", str(leader["pid"])], text=True)
        self.assertTrue(state.strip().startswith("Z"), "the original leader must remain unreaped: " + state)
        self.assertFalse((self.root / "plugin-side-effect").exists())
        self.release("plugin-release")
        self.wait_owner(0)
        self.assertTrue((self.root / "plugin-side-effect").exists())
        fact = self.control_fact()
        self.assertTrue(fact["leader_waited"])
        self.assertTrue(fact["group_exit_observed"])
        self.assertGreater(fact["group_exit_observed_monotonic"], 0)
        self.assertFalse(self.lock_held())

    def test_supervisor_start_delay_cannot_renew_original_absolute_deadline(self):
        self.start_owner(operation="control", timeout=0.2, supervisor_start_delay=0.3)
        self.wait_owner(1)
        self.assertIsNone(self.event("control.entered"))
        self.assertFalse((self.root / "control-side-effect").exists())
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "settled")
        self.assertFalse(fact["command_dispatched"])
        self.assertTrue(fact["timed_out"])
        self.assertEqual(fact["command_exit"], 124)

    def test_owner_sigkill_retains_flock_after_cli_leader_exit_until_plugin_exit(self):
        self.start_owner(operation="control", plugin=True)
        self.until(lambda: self.event("plugin.entered"), "plugin descendant")
        self.until(lambda: self.event("control.exit"), "CLI leader exit")
        self.owner_process.kill()
        self.assertEqual(self.owner_process.wait(timeout=3), -signal.SIGKILL)
        self.assertTrue(self.lock_held())
        self.assertFalse((self.root / "plugin-side-effect").exists())
        self.release("plugin-release")
        self.until(lambda: not self.lock_held(), "actual plugin exit and inherited flock release")
        self.assertIsNotNone(self.event("plugin.exit"))
        self.assertTrue((self.root / "plugin-side-effect").exists())
        self.assertTrue(self.control_fact()["group_exit_observed"])
        self.assertTrue(self.control_fact()["leader_waited"])
        self.assertTrue(self.control_fact()["interrupted"])

    def test_timed_out_term_ignoring_plugin_is_killed_before_lock_release(self):
        self.start_owner(operation="control", plugin=True, plugin_ignore_term=True, timeout=0.3)
        entered = self.until(lambda: self.event("plugin.entered"), "TERM-ignoring plugin")
        self.until(lambda: self.event("control.exit"), "CLI leader exit")
        time.sleep(0.4)
        self.assertTrue(self.lock_held(), "expiry is not proof of group exit")
        self.wait_owner(1)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "command-unknown")
        self.assertTrue(fact["timed_out"])
        self.assertTrue(fact["group_exit_observed"])
        self.assertTrue(fact["leader_waited"])
        self.assertFalse(self.lock_held())
        size = (self.root / "plugin-writes").stat().st_size
        time.sleep(0.05)
        self.assertEqual((self.root / "plugin-writes").stat().st_size, size)
        observed = subprocess.run(["/bin/ps", "-o", "stat=", "-p", str(entered["pid"])], capture_output=True, text=True)
        self.assertTrue(not observed.stdout.strip() or observed.stdout.strip().startswith("Z"), observed.stdout)
        self.assertFalse((self.root / "plugin-side-effect").exists())

    def test_nonzero_mutating_control_is_not_reinterpreted_as_completed_rpc(self):
        self.start_owner(operation="control", control_exit=17)
        self.wait_owner(1)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "command-unknown")
        self.assertEqual(fact["command_exit"], 17)
        owner = owner_for(self.root)
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            owner.activate({})
        self.assertEqual(caught.exception.code, "MachineOperationUnconfirmed")

    def test_observed_daemon_port_failure_can_retry_only_same_saved_intent(self):
        self.start_owner(operation="control", control_args=["compose", "up", "-d"], control_exit=1,
            saved_maintenance=True, control_stderr="Error response from daemon: failed to set up container networking: Bind for 127.0.0.1:28000 failed: port is already allocated")
        self.wait_owner(1)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "command-unknown")
        self.assertTrue(fact["group_exit_observed"])
        self.assertTrue(fact["leader_waited"])
        self.assertIsNotNone(self.event("control.exit"))
        owner = owner_for(self.root)
        checks = []
        with owner.maintenance_lock(), patch.object(owner.backend, "activation_preflight", side_effect=lambda target, source: checks.append(target["generation"])):
            owner.backend.settle_operations(owner.base)
        self.assertEqual(checks, ["generation-1"])
        self.assertEqual(self.control_fact()["phase"], "settled")
        self.assertFalse(self.control_fact()["command_succeeded"])
        self.assertEqual(owner.read_saved_state()["phase"], "maintenance")
        self.assertEqual(sum(event["event"] == "control.entered" for event in self.events()), 1)

    def test_nonzero_readonly_control_settles_failure_without_mutation_barrier(self):
        self.start_owner(operation="control", control_args=["info", "--format", "{{.ID}}"], control_exit=17)
        self.wait_owner(1)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "settled")
        self.assertFalse(fact["mutating"])
        self.assertFalse(fact["command_succeeded"])
        owner = owner_for(self.root)
        self.assertFalse(owner.backend.unsettled_operations(owner.base))

    def test_delayed_create_after_unknown_outcome_stays_stopped_and_can_be_recovered(self):
        self.start_owner(unknown_create=True, late_create=True)
        self.wait_owner(1)
        self.until(lambda: self.event("late.entered"), "fake daemon late create")
        self.assertEqual(self.fact()["phase"], "create-unknown")
        self.assertFalse((self.root / "container.json").exists())
        self.release("late-release")
        self.until(lambda: self.event("late.created"), "late stopped container")
        self.assertFalse(json.loads((self.root / "container.json").read_text())["State"]["Running"])
        self.assertIsNone(self.event("start.entered"))
        owner = owner_for(self.root)
        with owner.maintenance_lock():
            owner.backend.settle_operations(owner.base)
        self.assertFalse((self.root / "container.json").exists())
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertIsNone(self.event("start.entered"))
        self.assertEqual(self.event("rm.entered")["args"][-1], "container-exact-42")

    def test_foreign_container_is_never_started_or_removed_even_during_recovery(self):
        self.start_owner(foreign=True)
        self.wait_owner(1)
        self.assertEqual(self.fact()["phase"], "cleanup-failed")
        self.assertEqual(self.fact()["error"], "RuntimeIdentityMismatch")
        owner = owner_for(self.root)
        with owner.maintenance_lock():
            with self.assertRaises(runtime.RuntimeFailure) as caught:
                owner.backend.settle_operations(owner.base)
        self.assertEqual(caught.exception.code, "RuntimeIdentityMismatch")
        self.assertTrue((self.root / "container.json").exists())
        self.assertIsNone(self.event("start.entered"))
        self.assertIsNone(self.event("rm.entered"))

    def test_monitor_spawn_failure_closes_both_liveness_pipe_descriptors(self):
        self.configure()
        owner = owner_for(self.root)
        record = operation_record(self.root, owner)
        descriptors = []
        real_pipe = os.pipe

        def capture_pipe():
            pair = real_pipe()
            descriptors.extend(pair)
            return pair

        with owner.maintenance_lock():
            with patch.object(machine.os, "pipe", side_effect=capture_pipe), \
                    patch.object(machine.subprocess, "Popen", side_effect=OSError("fake monitor spawn failure")):
                with self.assertRaises(OSError):
                    owner.backend.run_mc(record, "echo fake")
        self.assertEqual(len(descriptors), 2)
        for fd in descriptors:
            try:
                os.fstat(fd)
            except OSError as error:
                self.assertEqual(error.errno, errno.EBADF)
            else:
                os.close(fd)
                self.fail("liveness pipe descriptor leaked after monitor spawn failure")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--fixture-owner":
        raise SystemExit(run_owner(Path(sys.argv[2])))
    unittest.main()
