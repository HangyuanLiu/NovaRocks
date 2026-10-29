# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Local supervisor correctness with real short children and original archives.

The fixtures substitute a short source-owned batch, not process exit or signal
observation. CI maintainers run these when local execution/control changes.
"""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import pwd
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from aliyun_ci import handoff, source_bundle
from aliyun_ci.instance import local_rust


class LocalRustTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.runtime = self.root / 'runtime'
        self.attempt = self.root / 'attempt'
        self.source = self.root / 'source'
        self.account = pwd.getpwuid(os.getuid())
        self.payload = {'execution_kind': 'local-rust', 'task': 'rust-workspace',
            'identity': {'run_id': 'local-test', 'run_attempt': 1},
            'deadline': (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            'attempt_root': str(self.attempt),
            'source': {'url': 'https://signed.invalid/private-capability'},
            'upload_urls': {'final': 'https://signed.invalid/upload',
                            'interrupted': 'https://signed.invalid/partial'},
            'watcher': {'cancel_budget_seconds': 2, 'poll_seconds': 1}}

    def install(self):
        return local_rust.install(self.payload, uid=os.getuid(), gid=os.getgid(),
            runtime=self.runtime, source_root=self.source, start_services=False)

    def test_install_keeps_urls_root_only_and_does_not_start_test(self):
        self.assertTrue(self.install()['installed'])
        self.assertEqual(0o600, (self.runtime / 'bootstrap.json').stat().st_mode & 0o777)
        self.assertFalse((self.attempt / 'batch.pid').exists())
        self.assertEqual(str(self.runtime),json.loads((self.runtime/'bootstrap.json').read_text())['pid_root'])
        self.assertNotIn('pid_root',self.payload)
        self.assertFalse(any(self.attempt.iterdir()))
        with self.assertRaises(FileExistsError):
            self.install()

    def test_nonzero_original_batch_still_archives_logs_and_source_without_capabilities(self):
        self.payload['build_jobs']=2
        self.install()
        self.source.mkdir()
        tools = self.source / 'tools/ci'
        tools.mkdir(parents=True)
        (tools / 'test-run.py').write_text("import json, pathlib, sys\nroot=pathlib.Path(sys.argv[-1]); root.mkdir()\n(root/'execution-index.json').write_text(json.dumps({'native_exit':7}))\nimport os; assert os.environ['CARGO_BUILD_JOBS']=='2'\nassert os.environ['CARGO_HTTP_MULTIPLEXING']=='false'\nprint('actual stdout'); print('actual stderr',file=sys.stderr)\nraise SystemExit(7)\n")
        # The real child sources its ordinary HOME environment, isolated per test.
        home = self.root / 'home'
        (home / '.config/novarocks').mkdir(parents=True)
        (home / '.config/novarocks/linux-env.sh').write_text(f'export PATH={Path(sys.executable).parent}:/usr/bin:/bin\n')
        from types import SimpleNamespace
        account = SimpleNamespace(pw_name=self.account.pw_name, pw_uid=os.getuid(),
            pw_gid=os.getgid(), pw_dir=str(home))
        packed = []
        def upload(attempt, *, kind, url, timeout):
            archive = handoff.pack(attempt, kind=kind, output=self.root / f'{kind}.tar.gz')
            fact = handoff.verify(archive)
            self.assertTrue(fact['manifest_ok'])
            packed.append((archive, kind))
            return {'uploaded': True, 'sha256': fact['archive_sha256']}
        result = local_rust.run(self.payload, runtime=self.runtime, source_root=self.source,
            account=account, source_preparer=lambda *_: {'sha': 'a' * 40, 'tree': 'b' * 40},
            tools_checker=lambda *args, **kwargs: {'action': 'check'}, uploader=upload)
        self.assertEqual(7, result['exit_code'])
        self.assertTrue(result['batch']['exited'])
        self.assertEqual('complete', result['state'])
        self.assertEqual('actual stdout\n', (self.attempt / 'stdout.log').read_text())
        self.assertEqual('actual stderr\n', (self.attempt / 'stderr.log').read_text())
        self.assertEqual({'native_exit': 7}, json.loads((self.attempt / 'rust/execution-index.json').read_text()))
        self.assertEqual('final', packed[0][1])
        self.assertEqual(result, json.loads((self.runtime / 'local-status.json').read_text()))
        self.assertNotIn('private-capability', (self.attempt / 'status.json').read_text())
        self.assertNotIn('signed.invalid', str(result))
        self.assertEqual(0o600, (self.runtime / 'batch.pid').stat().st_mode & 0o777)
        for name in ('status.json','source.json','tools-check.json','tools-check-result.json','stdout.log','stderr.log','batch.pid'):
            self.assertEqual(account.pw_uid,(self.attempt/name).stat().st_uid)
            self.assertEqual(0o640,(self.attempt/name).stat().st_mode & 0o777)

    def test_tools_failure_keeps_unknown_test_exit_and_upload_failure_independent(self):
        self.install()
        def bad_tools(*args, **kwargs):
            raise ValueError('https://signed.invalid/secret')
        def bad_upload(*args, **kwargs):
            raise OSError('https://signed.invalid/other-secret')
        result = local_rust.run(self.payload, runtime=self.runtime, source_root=self.source,
            account=self.account, source_preparer=lambda *_: {'sha': 'a' * 40},
            tools_checker=bad_tools, uploader=bad_upload)
        self.assertEqual(1, result['tools_check_exit'])
        self.assertIsNone(result['exit_code'])
        self.assertIsNone(result['handoff']['uploaded'])
        self.assertEqual('ValueError', result['platform_error'])
        self.assertNotIn('signed.invalid', json.dumps(result))
        self.assertFalse((self.attempt / 'batch.pid').exists())

    def test_sigterm_reaches_original_child_and_real_exit_is_observed(self):
        self.runtime.mkdir()
        self.attempt.mkdir()
        self.source.mkdir()
        child = self.root / 'child.py'
        child.write_text("import pathlib,signal,sys,time\ndef stop(*args):\n pathlib.Path(sys.argv[1]).write_text('received SIGTERM'); raise SystemExit(23)\nsignal.signal(signal.SIGTERM,stop)\npathlib.Path(sys.argv[2]).write_text('ready')\nwhile True: time.sleep(.05)\n")
        supervisor = self.root / 'supervisor.py'
        supervisor.write_text("import json,os,pathlib,pwd,sys\nfrom aliyun_ci.instance.local_rust import execute_batch\nr=pathlib.Path(sys.argv[1])\nv=execute_batch([sys.executable,str(r/'child.py'),str(r/'signal'),str(r/'ready')],source_root=r/'source',attempt_root=r/'attempt',runtime=r/'runtime',account=pwd.getpwuid(os.getuid()),environment=os.environ.copy())\n(r/'result').write_text(json.dumps(v))\n")
        environment = os.environ.copy()
        environment['PYTHONPATH'] = str(Path(__file__).resolve().parents[2])
        process = subprocess.Popen([sys.executable, str(supervisor), str(self.root)], env=environment)
        self.addCleanup(lambda: process.kill() if process.poll() is None else None)
        end = time.monotonic() + 10
        while not (self.root / 'ready').exists() and time.monotonic() < end:
            if process.poll() is not None:
                self.fail(f'supervisor exited early: {process.returncode}')
            time.sleep(.02)
        self.assertTrue((self.root / 'ready').exists())
        original_pid = int((self.runtime / 'batch.pid').read_text())
        process.send_signal(signal.SIGTERM)
        self.assertEqual(0, process.wait(timeout=10))
        fact = json.loads((self.root / 'result').read_text())
        self.assertEqual(23, fact['exit_code'])
        self.assertEqual(original_pid, fact['pid'])
        self.assertTrue(fact['exited'])
        self.assertTrue(fact['cancel_requested'])
        self.assertEqual('received SIGTERM', (self.root / 'signal').read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(original_pid, 0)

    def test_compact_manifest_pointer_verifies_remote_manifest_and_self_contained_bundle(self):
        import hashlib
        import io
        repo = self.root / 'repo'
        repo.mkdir()
        for args in (('init', '-q'), ('config', 'user.name', 'Test'),
                     ('config', 'user.email', 'test@example.invalid')):
            subprocess.run(['git', '-C', str(repo), *args], check=True)
        (repo / 'source.rs').write_text('fn example() {}\n')
        subprocess.run(['git', '-C', str(repo), 'add', '.'], check=True)
        subprocess.run(['git', '-C', str(repo), 'commit', '-qm', 'Exact source'], check=True)
        manifest = source_bundle.create(repo, self.root / 'carrier')
        raw = (self.root / 'carrier/source.json').read_bytes()
        bundle = (self.root / 'carrier/source.bundle').read_bytes()
        self.payload['source'] = {key: manifest[key] for key in ('sha', 'tree', 'bundle_sha256')}
        self.payload['source'].update(url='https://signed.invalid/bundle',
            manifest_url='https://signed.invalid/manifest', manifest_sha256=hashlib.sha256(raw).hexdigest())
        self.install()
        values = {'https://signed.invalid/bundle': bundle, 'https://signed.invalid/manifest': raw}
        fact = local_rust.prepare_source(self.payload, self.source, self.runtime,
            opener=lambda url, **kwargs: io.BytesIO(values[url]))
        self.assertEqual(manifest['sha'], fact['sha'])
        self.assertEqual('fn example() {}\n', (self.source / 'source.rs').read_text())
        self.assertNotIn('private', (self.source / '.git/config').read_text())

    def test_native_cancel_budget_expiry_keeps_actual_exit_unknown(self):
        self.runtime.mkdir()
        self.attempt.mkdir()
        self.source.mkdir()
        # A cancellation intent precedes the signal. This child deliberately
        # remains alive: the supervisor may not manufacture a cleanup success.
        ready = self.root / 'ready'
        children = []
        def spawn(*args, **kwargs):
            child = subprocess.Popen(*args, **kwargs)
            children.append(child)
            end = time.monotonic() + 10
            while not ready.exists() and time.monotonic() < end:
                time.sleep(.01)
            self.assertTrue(ready.exists())
            (self.runtime / 'cancellation.json').write_text('{"intent":true}')
            return child
        script = 'import signal,time,pathlib,sys; signal.signal(signal.SIGTERM,signal.SIG_IGN); pathlib.Path(sys.argv[1]).touch(); time.sleep(30)'
        result = local_rust.execute_batch([sys.executable, '-c', script, str(ready)],
            source_root=self.source, attempt_root=self.attempt, runtime=self.runtime,
            account=self.account, environment=os.environ.copy(), cancel_budget_seconds=.1, popen=spawn)
        try:
            self.assertIsNone(result['exit_code'])
            self.assertFalse(result['exited'])
            self.assertTrue(result['cancel_requested'])
            os.kill(result['pid'], 0)
        finally:
            children[0].kill()
            children[0].wait(timeout=5)


    def test_cancellation_before_batch_launch_does_not_start_work(self):
        self.runtime.mkdir()
        self.attempt.mkdir()
        (self.runtime / 'cancellation.json').write_text('{"intent":true}')
        def unexpected_spawn(*args, **kwargs):
            self.fail('preexisting cancellation must prevent native batch launch')
        result = local_rust.execute_batch([], source_root=self.source,
            attempt_root=self.attempt, runtime=self.runtime, account=self.account,
            environment={}, popen=unexpected_spawn)
        self.assertIsNone(result['exit_code'])
        self.assertIsNone(result['pid'])
        self.assertTrue(result['cancel_requested'])
        self.assertFalse((self.runtime / 'batch.pid').exists())


    def test_atomic_status_never_follows_runner_created_symlinks(self):
        self.attempt.mkdir()
        outside = self.root / 'root-owned-target'
        outside.write_text('must remain unchanged')
        target = self.attempt / 'status.json'
        target.symlink_to(outside)
        # This was the predictable root temporary name in the reported bug.
        (self.attempt / 'status.json.new').symlink_to(outside)
        local_rust._atomic(target, {'state': 'complete'}, mode=0o640)
        self.assertEqual('must remain unchanged', outside.read_text())
        self.assertFalse(target.is_symlink())
        self.assertEqual({'state': 'complete'}, json.loads(target.read_text()))
        self.assertEqual(0o640, target.stat().st_mode & 0o777)
        self.assertFalse(list(self.attempt.glob('.status.json.*')))

    def test_upload_timeout_uses_remaining_original_capability_deadline(self):
        self.install()
        clock = datetime(2026, 10, 29, 8, 0, tzinfo=timezone.utc)
        self.payload['deadline'] = (clock + timedelta(seconds=187)).isoformat()
        timeouts = []
        def no_tools(*args, **kwargs):
            raise ValueError('tools unavailable')
        def upload(attempt, *, kind, url, timeout):
            timeouts.append(timeout)
            return {'uploaded': False}
        result = local_rust.run(self.payload, runtime=self.runtime, source_root=self.source,
            account=self.account, source_preparer=lambda *_: {'sha': 'a' * 40},
            tools_checker=no_tools, uploader=upload, now=lambda: clock)
        self.assertEqual([187], timeouts)
        self.assertFalse(result['handoff']['uploaded'])
        self.assertIsNone(result['exit_code'])
