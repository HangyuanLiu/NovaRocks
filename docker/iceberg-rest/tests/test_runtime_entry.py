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

"""Immutable entry, real shell boundaries and unchanged private lifecycle contracts."""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import fixture_runtime as runtime
import machine_runtime as machine
import entry_publication as publication
import runtime_entry as entry
from test_machine_runtime import Backend, Sweeper, bom


class RendererCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.workspace = self.root / 'workspace'
        self.workspace.mkdir()
        self.entry = self.root / 'runtime' / 'test-worktree'
        self.backend = Backend()
        self.owner = machine.MachineOwner(self.root / 'machine', backend=self.backend, sweeper_factory=Sweeper)
        self.snapshot = self.owner.activate(self.owner.required_definition({}, bom()))
        self.config = entry.request_config(self.workspace, entry.load_settings(HERE / 'shared.env'), HERE / 'shared.env', self.entry, shared=True, update_current=False)
        self.config['fixture_inputs'] = {'bom': str(self.root / 'bom.json'), 'lock_sha256': bom()['lock_sha256'], 'verified': True}

    def bind(self, inputs=None):
        return entry.attach_entry(self.owner, 'test-worktree', self.entry, self.config, inputs or bom())

    def environment(self, path, **extra):
        output = subprocess.check_output(['bash', '-c', 'source "$1"; python3 -c "import os,json; print(json.dumps(dict(os.environ)))"', '_', str(path)], env={**os.environ, **extra}, text=True)
        return json.loads(output)


class RendererTests(RendererCase):
    def test_full_snapshot_without_owner_facade_and_stable_runtime_data(self):
        first = self.bind()
        directory = Path(first['published_dir'])
        manifest = json.loads((directory / 'manifest.json').read_text())
        self.assertEqual(manifest['runtime']['machine']['generation'], self.snapshot['generation'])
        self.assertNotIn('catalog', manifest['runtime'])
        self.assertNotIn('object_store', manifest['runtime'])
        self.assertEqual(manifest['runtime']['producer_receipt'], self.snapshot['producer_receipt'])
        env = self.environment(directory / 'env.sh', REST_IMAGE='foreign')
        self.assertEqual(env['NOVA_ENV_REST_ENV_FILE'], str(directory / 'env.sh'))
        self.assertEqual(env['NOVAROCKS_STATE_STORE_PATH'], str(self.entry / 'frontend-state.sqlite'))
        self.assertEqual(env['NOVA_FIXTURE_MACHINE_GENERATION'], self.snapshot['generation'])
        self.assertEqual(env['AWS_S3_ACCESS_KEY_ID'], self.snapshot['credentials']['access_key'])
        self.assertEqual(env['NOVAROCKS_PAIMON_DOCKER_NETWORK'], self.snapshot['network'])
        fe = tomllib.loads((directory / 'fe.toml').read_text())
        self.assertEqual(fe['state_store']['path'], str(self.entry / 'frontend-state.sqlite'))
        for name in runtime.ENTRY_FILES:
            self.assertTrue((directory / name).is_file(), name)
            self.assertNotIn('.prepare-', (directory / name).read_text())

    def test_same_configuration_reuses_exact_publication_and_env_bytes(self):
        first = self.bind()
        before = (self.entry / 'env.sh').read_bytes()
        second = self.bind()
        self.assertEqual(first['published_dir'], second['published_dir'])
        self.assertEqual(before, (self.entry / 'env.sh').read_bytes())
        self.assertEqual(self.backend.events, ['ensure'])

    def test_new_activation_changes_identity_and_old_snapshot_remains_immutable(self):
        first = self.bind()
        old_bytes = (Path(first['published_dir']) / 'env.sh').read_bytes()
        self.owner.activate(self.owner.required_definition({}, bom()), upgrade=True)
        second = self.bind()
        self.assertNotEqual(first['published_dir'], second['published_dir'])
        self.assertEqual((Path(first['published_dir']) / 'env.sh').read_bytes(), old_bytes)
        self.assertNotEqual(old_bytes, (self.entry / 'env.sh').read_bytes())

    def test_version_failure_does_not_change_services_and_clears_ready_entry(self):
        first = self.bind()
        before = copy.deepcopy(self.backend.events)
        changed = bom(); changed['derived_images']['rest-mv']['definition_sha256'] = 'different'
        with self.assertRaisesRegex(runtime.RuntimeFailure, 'MachineVersionMismatch'):
            self.bind(changed)
        self.assertFalse(publication.read(self.entry)['ready'])
        self.assertEqual(self.backend.events, before)
        self.assertTrue((Path(first['published_dir']) / 'env.sh').is_file())

    def test_identity_observation_failure_disables_discovery_without_mutating_services(self):
        first = self.bind()
        before = copy.deepcopy(self.backend.events)
        with mock.patch.object(self.backend, "healthy", side_effect=runtime.RuntimeFailure("RuntimeIdentityMismatch", "replaced service")):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "RuntimeIdentityMismatch"):
                self.bind()
        self.assertFalse(publication.read(self.entry)["ready"])
        self.assertEqual(self.backend.events, before)
        self.assertTrue((Path(first["published_dir"]) / "env.sh").is_file())

    def test_renderer_and_interrupted_commit_preserve_previous_complete_publication(self):
        first = self.bind()
        invalid = copy.deepcopy(self.config); del invalid['local_ports']['mysql']
        with self.assertRaises(KeyError):
            entry.attach_entry(self.owner, 'test-worktree', self.entry, invalid, bom())
        self.assertEqual(publication.read(self.entry), first)
        changed = copy.deepcopy(first); changed['config']['benchmark']['build_timeout_seconds'] = 120
        for point in ('publication.prepared', 'publication.before_swap'):
            def fail(name):
                if name == point:
                    raise InterruptedError('injected commit interruption')
            with self.assertRaises(InterruptedError):
                publication.publish(self.entry, changed, entry.render_entry, hook=fail)
            self.assertEqual(publication.read(self.entry), first)

    def test_corrupt_publication_fails_closed(self):
        first = self.bind()
        (Path(first['published_dir']) / 'be.toml').write_text('corrupt')
        with self.assertRaisesRegex(runtime.RuntimeFailure, 'IncompletePublication'):
            publication.read(self.entry)
        with self.assertRaisesRegex(runtime.RuntimeFailure, 'IncompletePublication'):
            self.bind()

    def test_legacy_owner_metadata_is_not_read_during_explicit_prepare(self):
        legacy = self.entry / 'publications' / 'legacy'
        legacy.mkdir(parents=True)
        (legacy / 'binding.json').write_text('unreadable retired owner data')
        (legacy / 'env.sh').write_text('export NOVA_ENV_READY=true\nexport AWS_S3_ENDPOINT=stale\n')
        (self.entry / 'published').symlink_to('publications/legacy')
        (self.entry / 'env.sh').symlink_to('published/env.sh')
        config = entry.request_config(self.workspace, entry.load_settings(HERE / 'shared.env'), HERE / 'shared.env', self.entry, shared=True, update_current=False)
        result = entry.prepare_entry('test-worktree', self.entry, config)
        self.assertFalse(result['ready'])
        self.assertNotIn('AWS_S3_ENDPOINT', self.environment(self.entry / 'env.sh'))
        self.assertTrue((legacy / 'binding.json').is_file())

    def test_cli_returns_only_public_identity_and_input_failure_disables_entry(self):
        environment = {**os.environ, 'NOVAROCKS_WORKSPACE_ROOT': str(self.workspace),
            'NOVA_ENV_CONFIG_FILE': str(HERE / 'shared.env'), 'NOVA_ENV_UPDATE_CURRENT': 'true'}
        def verified(config):
            config['fixture_inputs'].update(lock_sha256=bom()['lock_sha256'], verified=True)
            return bom()
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.object(machine, 'MachineOwner', return_value=self.owner), mock.patch.object(entry, 'verify_inputs', side_effect=verified):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(entry.worktree_main(['up']), 0)
            public = json.loads(output.getvalue())
            self.assertEqual(set(public), {'ready', 'published_dir', 'machine'})
            self.assertNotIn('credentials', public['machine'])
            self.assertNotIn(self.snapshot['credentials']['secret_key'], output.getvalue())
            with mock.patch.object(entry, 'verify_inputs', side_effect=runtime.RuntimeFailure('FixturePrerequisiteMissing', 'absent')), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(entry.worktree_main(['up']), 75)
            actual_entry = Path(public['published_dir']).parent.parent
            self.assertFalse(publication.read(actual_entry)['ready'])

    def test_prepare_is_unavailable_and_offline_even_after_ready(self):
        self.bind()
        with mock.patch.object(machine, 'MachineOwner', side_effect=AssertionError('offline invoked Docker owner')):
            result = entry.prepare_entry('test-worktree', self.entry, self.config)
        self.assertFalse(result['ready'])
        env = self.environment(self.entry / 'env.sh', NOVA_ENV_REST_PORT='stale', NOVA_FIXTURE_MACHINE_GENERATION='stale')
        self.assertEqual(env['NOVA_ENV_READY'], 'false')
        self.assertNotIn('NOVA_ENV_REST_PORT', env)
        self.assertNotIn('NOVA_FIXTURE_MACHINE_GENERATION', env)

    def test_attach_publication_commit_holds_machine_lock(self):
        original = entry.render_entry
        def renderer(context, staging):
            other = machine.MachineOwner(self.owner.root, backend=self.backend, timeout=0.001)
            with self.assertRaisesRegex(runtime.RuntimeFailure, 'MachineMaintaining'):
                with other.maintenance_lock(deadline=entry.time.monotonic()):
                    self.fail('maintenance entered during publication')
            original(context, staging)
        with mock.patch.object(entry, 'render_entry', side_effect=renderer):
            self.bind()


class SharedEnvironmentTests(RendererCase):
    def test_two_worktrees_share_machine_and_benchmark_but_not_worktree_prefixes(self):
        first = self.bind()
        other_entry = self.entry.parent / 'other-worktree'
        other_config = copy.deepcopy(self.config)
        other_config.update(env_id='other-worktree', workspace_root=str(self.root / 'other'))
        other_config['warehouses'] = {name: value.replace('test-worktree', 'other-worktree') for name, value in self.config['warehouses'].items()}
        second = entry.attach_entry(self.owner, 'other-worktree', other_entry, other_config, bom())
        a, b = (self.environment(Path(p['published_dir']) / 'env.sh') for p in (first, second))
        self.assertEqual(a['NOVA_FIXTURE_MACHINE_ID'], b['NOVA_FIXTURE_MACHINE_ID'])
        self.assertEqual(a['AWS_S3_ENDPOINT'], b['AWS_S3_ENDPOINT'])
        self.assertEqual(a['NOVA_ENV_SHARED_BENCHMARK_ROOT'], b['NOVA_ENV_SHARED_BENCHMARK_ROOT'])
        self.assertNotEqual(a['CATALOG_WAREHOUSE_URI'], b['CATALOG_WAREHOUSE_URI'])
        self.assertEqual(self.backend.events, ['ensure'])

    def test_declaration_checkouts_share_consumer_entry_lock_and_local_ports(self):
        fakebin = self.root / 'bin'; fakebin.mkdir()
        marker = self.root / 'unexpected-docker'
        docker = fakebin / 'docker'; docker.write_text('#!/bin/sh\ntouch ' + str(marker) + '\nexit 99\n'); docker.chmod(0o755)
        results = []
        for label in ('definition-a', 'definition-b'):
            source = self.root / label / 'docker/iceberg-rest'; source.mkdir(parents=True)
            for name in ('up.sh', 'fixture_runtime.py', 'machine_runtime.py', 'entry_publication.py', 'runtime_entry.py', 'shared.env'):
                shutil.copy2(HERE / name, source / name)
            environment = {**os.environ, 'PATH': str(fakebin) + ':' + os.environ['PATH'],
                'NOVAROCKS_WORKSPACE_ROOT': str(self.workspace), 'NOVA_ENV_CONFIG_FILE': str(source / 'shared.env')}
            process = subprocess.run(['bash', str(source / 'up.sh'), '--prepare-only'], env=environment, capture_output=True, text=True)
            self.assertEqual(process.returncode, 0, process.stderr)
            result = json.loads(process.stdout)
            actual_entry = Path(result['published_dir']).parent.parent
            saved = publication.read(actual_entry)
            results.append((actual_entry, saved['config'], (actual_entry / '.entry.lock').stat().st_ino))
            self.assertFalse((source / 'runtime').exists())
        self.assertEqual(results[0][0], results[1][0])
        self.assertEqual(results[0][1]['local_ports'], results[1][1]['local_ports'])
        self.assertEqual(results[0][1]['native_trust'], results[1][1]['native_trust'])
        self.assertEqual(results[0][2], results[1][2])
        self.assertFalse(marker.exists())

    def test_real_shell_prepare_and_status_are_offline_and_clear_ambient_endpoints(self):
        fakebin = self.root / 'bin'; fakebin.mkdir()
        marker = self.root / 'docker-was-called'
        docker = fakebin / 'docker'; docker.write_text('#!/bin/sh\ntouch ' + str(marker) + '\nexit 99\n'); docker.chmod(0o755)
        environment = {**os.environ, 'PATH': str(fakebin) + ':' + os.environ['PATH'],
            'NOVAROCKS_WORKSPACE_ROOT': str(self.workspace), 'NOVA_ENV_CONFIG_FILE': str(HERE / 'shared.env')}
        result = subprocess.run(['bash', str(HERE / 'up.sh'), '--prepare-only'], env=environment, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        env = self.environment(Path(value['published_dir']) / 'env.sh', NOVA_ENV_REST_PORT='9999')
        self.assertNotIn('NOVA_ENV_REST_PORT', env)
        self.assertFalse(value['ready'])
        self.assertNotIn('config', value)  # CLI output does not leak credential material.
        status = subprocess.run(['bash', str(HERE / 'status.sh')], env=environment, capture_output=True, text=True)
        self.assertEqual(status.returncode, 0, status.stderr)
        self.assertEqual(json.loads(status.stdout)['published_dir'], value['published_dir'])
        self.assertFalse(marker.exists())


class HelperEntryTests(RendererCase):
    def sweep_python(self, fakebin, log, *, fail=False):
        shim = fakebin / "python3"
        shim.write_text("#!" + sys.executable + "\n" +
            "import json,os,sys\nfrom pathlib import Path\n" +
            "if len(sys.argv)>2 and sys.argv[1:3]==[" + repr(str(HERE / "fixture_run.py")) + ", 'sweep-start']:\n" +
            " with Path(" + repr(str(log)) + ").open('a') as out: out.write(json.dumps({'kind':'sweep','args':sys.argv[2:],'stdin':'','foreign':os.environ.get('REST_IMAGE'),'context':json.loads(os.environ['NOVA_FIXTURE_RUN_CONTEXT'])})+'\\n')\n" +
            " print(json.dumps({'complete':" + repr(not fail) + ",'pending':False,'errors':" +
            repr([{'code':'ScratchCleanupFailed'}] if fail else []) + ",'removed':[]}))\n" +
            " sys.exit(" + ("75" if fail else "0") + ")\n" +
            "os.execv(" + repr(sys.executable) + ", [" + repr(sys.executable) + ", *sys.argv[1:]])\n")
        shim.chmod(0o755)

    def smoke_docker(self, first, mode="success"):
        log = self.root / "smoke-docker.calls"
        fakebin = self.root / "smoke-bin"; fakebin.mkdir(exist_ok=True)
        state = self.root / "smoke-worker-status"
        docker = fakebin / "docker"
        docker.write_text("#!/usr/bin/env python3\n" + "import json,sys\nfrom pathlib import Path\n" +
            "log=Path(" + repr(str(log)) + "); state=Path(" + repr(str(state)) + "); mode=" + repr(mode) + "\n" +
            "args=sys.argv[1:]; data=sys.stdin.buffer.read().decode('utf-8')\n" +
            "with log.open('a') as out: out.write(json.dumps({'args':args,'stdin':data})+'\\n')\n" +
            "if args[-3:]==['ps','-q','spark']: print(" + repr(first["machine"]["containers"]["spark"]) + "); sys.exit(0)\n" +
            "if any('value = json.loads' in a for a in args): print(json.dumps({'returncode':int(state.read_text()),'child_waited':True})); sys.exit(0)\n" +
            "if any('start_new_session=True' in a for a in args):\n" +
            " code=17 if mode=='sql-failure' and any('spark_sql_bin=' in a for a in args) else (23 if mode=='cleanup-failure' and any('shutil.rmtree(folder)' in a for a in args) else 0)\n" +
            " state.write_text(str(code)); sys.exit(code)\n")
        docker.chmod(0o755)
        self.sweep_python(fakebin, log, fail=mode == "sweep-failure")
        return log, {**os.environ, "PATH":str(fakebin)+":"+os.environ["PATH"],
                     "NOVA_ENV_REST_ENV_FILE":str(Path(first["published_dir"])/"env.sh")}

    def test_generated_shared_smoke_expands_exact_original_run_and_purges_its_own_names(self):
        import fixture_run
        first = self.bind()
        publication_dir = Path(first["published_dir"])
        template = (publication_dir/"spark-iceberg-v3-smoke.sql").read_text()
        self.assertTrue(template.startswith("-- novarocks-generated-shared-spark-v3-smoke-v1\n"))
        self.assertNotIn("nr_v3",template)
        contexts=[]
        for _ in range(2):
            with mock.patch.dict(os.environ,{},clear=True): contexts.append(fixture_run.admit(publication_dir/"env.sh")[2].as_dict())
        log,environment = self.smoke_docker(first)
        for context in contexts:
            result = subprocess.run(["bash",str(HERE/"spark-sql.sh")],env={**environment,"NOVA_FIXTURE_RUN_CONTEXT":json.dumps(context)},input="",capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
        calls=[json.loads(line) for line in log.read_text().splitlines()]
        sweeps=[call for call in calls if call.get("kind") == "sweep"]
        self.assertEqual([call["context"] for call in sweeps], contexts)
        self.assertTrue(all(call["args"] == ["sweep-start", "--env-file", str(publication_dir / "env.sh")] for call in sweeps))
        for index, call in enumerate(calls):
            if call.get("kind") == "sweep":
                self.assertLess(index, next(i for i in range(index + 1, len(calls))
                    if any("start_new_session=True" in argument for argument in calls[i]["args"])))
        queries=[call["stdin"] for call in calls if call["stdin"].startswith("-- novarocks-generated-shared-spark-v3-smoke-v1")]
        self.assertEqual(len(queries),2)
        for text,context in zip(queries,contexts):
            namespace=context["identity"]["run_id"]+"_smoke"
            location=context["identity"]["scope_root"]+"/rest/"+namespace
            self.assertEqual(len(namespace),63)
            self.assertIn("CREATE NAMESPACE ice_rest."+namespace+" LOCATION '"+location+"';",text)
            self.assertIn("LOCATION '"+location+"/spark_v3_smoke'",text)
            self.assertIn("DROP TABLE ice_rest."+namespace+".spark_v3_smoke PURGE;",text)
            self.assertTrue(text.endswith("DROP NAMESPACE ice_rest."+namespace+";\n"))
            self.assertIn("(3, 'spark-v3-c', 'alpha', TIMESTAMP '2026-05-07 00:02:00')",text)
            self.assertIn("SELECT * FROM ice_rest."+namespace+".spark_v3_smoke ORDER BY id;",text)
            self.assertNotIn("__NOVA_SMOKE",text)
        self.assertNotIn(contexts[0]["identity"]["run_id"],queries[1])
        self.assertEqual((publication_dir/"spark-iceberg-v3-smoke.sql").read_text(),template)
        for context in contexts: self.assertNotIn(context["identity"]["run_id"],(publication_dir/"env.sh").read_text())

    def test_ordinary_smoke_named_sql_is_copied_without_token_or_namespace_rewrite(self):
        first=self.bind(); log,environment=self.smoke_docker(first)
        source=self.root/"spark-iceberg-v3-smoke.sql"
        text=b"-- novarocks-generated-shared-spark-v3-smoke-v1\r\nSELECT '__NOVA_SMOKE_NAMESPACE__', 'ice_rest.nr_v3';\r\n"
        source.write_bytes(text)
        result=subprocess.run(["bash",str(HERE/"spark-sql.sh"),str(source)],env=environment,input="",capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn(text.decode(),[json.loads(line)["stdin"] for line in log.read_text().splitlines()])

    def test_shared_publication_without_scoped_marker_is_rejected_before_docker(self):
        first=self.bind(); log,environment=self.smoke_docker(first)
        generated=Path(first["published_dir"])/"spark-iceberg-v3-smoke.sql"
        generated.write_text("CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_v3;\n")
        result=subprocess.run(["bash",str(HERE/"spark-sql.sh")],env=environment,input="",capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn("shared generated smoke has no scoped template",result.stderr)
        self.assertFalse(log.exists())

    def test_failed_shared_smoke_retains_diagnostics_and_does_not_claim_remote_cleanup(self):
        first=self.bind(); log,environment=self.smoke_docker(first,"sql-failure")
        result=subprocess.run(["bash",str(HERE/"spark-sql.sh")],env=environment,input="",capture_output=True,text=True)
        self.assertEqual(result.returncode,17,result.stderr)
        self.assertIn("remote_cleanup=NOT_CONFIRMED",result.stderr)
        self.assertIn("diagnostics=/tmp/novarocks-fixture-data-nr_scratch_",result.stderr)
        calls=[json.loads(line) for line in log.read_text().splitlines()]
        self.assertFalse(any("shutil.rmtree(folder)" in arg for call in calls for arg in call["args"]))

    def test_successful_shared_smoke_cannot_hide_local_cleanup_failure(self):
        first=self.bind(); _,environment=self.smoke_docker(first,"cleanup-failure")
        result=subprocess.run(["bash",str(HERE/"spark-sql.sh")],env=environment,input="",capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn("ContainerCleanupFailed",result.stderr)

    def test_startup_sweep_failure_blocks_both_helpers_before_any_writer(self):
        import fixture_run
        first = self.bind()
        publication_dir = Path(first["published_dir"])
        with mock.patch.dict(os.environ, {}, clear=True):
            context = fixture_run.admit(publication_dir / "env.sh")[2].as_dict()
        log, environment = self.smoke_docker(first, "sweep-failure")
        for helper, suffix in (("spark-sql.sh", "sql"), ("spark-shell.sh", "scala")):
            with self.subTest(helper=helper):
                log.unlink(missing_ok=True)
                source = self.root / ("query." + suffix)
                source.write_text("SELECT 1;\n" if suffix == "sql" else "println(1)\n")
                result = subprocess.run(["bash", str(HERE / helper), str(source)],
                    env={**environment, "NOVA_FIXTURE_RUN_CONTEXT":json.dumps(context)},
                    input="", capture_output=True, text=True)
                self.assertEqual(result.returncode, 75, result.stderr)
                self.assertIn("ScratchCleanupFailed", result.stderr)
                calls = [json.loads(line) for line in log.read_text().splitlines()]
                self.assertEqual(len(calls), 1, "failed sweep must block Docker and temporary writer setup")
                self.assertEqual(calls[0]["kind"], "sweep")
                self.assertEqual(calls[0]["context"], context)
                self.assertEqual(calls[0]["args"], ["sweep-start", "--env-file", str(publication_dir / "env.sh")])

    def test_helper_uses_exact_published_compose_and_discards_ambient_overrides(self):
        import fixture_run
        first = self.bind()
        log = self.root / 'docker.calls'
        fakebin = self.root / 'bin'; fakebin.mkdir()
        docker = fakebin / 'docker'
        docker.write_text('#!/usr/bin/env python3\nimport json,os,sys\nfrom pathlib import Path\nwith Path(' + repr(str(log)) + ').open("a") as f: f.write(json.dumps({"args":sys.argv[1:],"foreign":os.environ.get("REST_IMAGE")})+"\\n")\n'
            'if sys.argv[-3:]==["ps","-q","spark"]: print(' + repr(first['machine']['containers']['spark']) + ')\n'
            'if "python3" in sys.argv and any("value = json.loads" in value for value in sys.argv):\n'
            ' print(json.dumps({"returncode":0,"child_waited":True}))\n')
        docker.chmod(0o755)
        self.sweep_python(fakebin, log)
        sql = self.root / 'query.sql'; sql.write_text('SELECT 1;\n')
        scala = self.root / 'query.scala'; scala.write_text('println(1)\n')
        environment = {**os.environ, 'PATH': str(fakebin) + ':' + os.environ['PATH'],
            'NOVA_ENV_REST_ENV_FILE': str(Path(first['published_dir']) / 'env.sh'), 'REST_IMAGE': 'foreign'}
        with mock.patch.dict(os.environ, {}, clear=True):
            context = fixture_run.admit(Path(first['published_dir']) / 'env.sh')[2].as_dict()
        environment['NOVA_FIXTURE_RUN_CONTEXT'] = json.dumps(context)
        for helper, source in [('spark-sql.sh', sql), ('spark-shell.sh', scala)]:
            result = subprocess.run(['bash', str(HERE / helper), str(source)], env=environment, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        self.assertTrue(calls)
        sweeps = [call for call in calls if call.get('kind') == 'sweep']
        self.assertEqual(len(sweeps), 2)
        self.assertTrue(all(call['context'] == context for call in sweeps))
        self.assertTrue(all(call['args'] == ['sweep-start', '--env-file', str(Path(first['published_dir']) / 'env.sh')] for call in sweeps))
        self.assertEqual(calls[0]['kind'], 'sweep')
        for index, call in enumerate(calls):
            if call.get('kind') == 'sweep':
                self.assertLess(index, next(i for i in range(index + 1, len(calls))
                    if any('start_new_session=True' in argument for argument in calls[i]['args'])))
        workers = [call for call in calls if any('start_new_session=True' in argument for argument in call['args'])]
        settlements = [call for call in calls if any('value = json.loads' in argument for argument in call['args'])]
        self.assertTrue(workers)
        self.assertEqual(len(workers), len(settlements))
        self.assertTrue(any('spark-sql' in ' '.join(call['args']) for call in workers))
        self.assertTrue(any('spark-shell' in ' '.join(call['args']) for call in workers))
        lookups = [call for call in calls if call['args'][:1] == ['compose']]
        self.assertEqual(len(lookups), len(workers))
        for call in lookups:
            self.assertIn(first['machine']['compose_file'], call['args'])
            self.assertIn(first['machine']['compose_env'], call['args'])
            self.assertEqual(call['args'][-3:], ['ps', '-q', 'spark'])
        for call in calls:
            if call['args'][:1] == ['exec']:
                self.assertIn(first['machine']['containers']['spark'], call['args'])
            if call.get('kind') == 'sweep':
                self.assertEqual(call['foreign'], 'foreign')
            else:
                self.assertIsNone(call['foreign'])
        entry.release_entry('test-worktree', self.entry)
        environment['NOVA_ENV_REST_ENV_FILE'] = str(self.entry / 'env.sh')
        result = subprocess.run(['bash', str(HERE / 'spark-sql.sh'), str(sql)], env=environment, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('fixture is not ready', result.stderr)


class DownPurgeTests(RendererCase):
    def test_release_keeps_sqlite_and_does_not_touch_shared_machine_or_s3(self):
        first = self.bind()
        lock = self.entry / '.entry.lock'; inode = lock.stat().st_ino
        state = self.entry / 'frontend-state.sqlite'; state.write_text('stable')
        before = copy.deepcopy(self.backend.events)
        result = entry.release_entry('test-worktree', self.entry, purge=True)
        self.assertFalse(result['ready'])
        self.assertEqual(state.read_text(), 'stable')
        self.assertEqual(lock.stat().st_ino, inode)
        self.assertEqual(self.backend.events, before)
        self.assertTrue(result['retained_publications'])
        self.assertTrue((Path(first['published_dir']) / 'env.sh').is_file())


class IsolatedEntryTests(RendererCase):
    def isolated(self, profile='stock'):
        config = copy.deepcopy(self.config)
        config.update(shared_docker=False, update_current=False)
        inputs = bom(); inputs['derived_images']['iceberg-spark']['image_id'] = 'sha256:spark'
        return entry.render_isolated_stack(inputs, 'nr-isolated-rest-test', {'minio': 19101, 'minio_console': 20101, 'rest': 21101, 'rest_mv': 24101, 'spark': 22101, 'control': 23101}, self.entry, config,
            profile=profile, hook_image='publication:exact' if profile != 'stock' else None)

    def test_stock_and_hook_are_initial_models_from_same_templates(self):
        sentinel = self.entry.parent / 'current'; sentinel.parent.mkdir(parents=True)
        sentinel.symlink_to('existing-worktree')
        result = self.isolated()
        entry.render_entry(result['context'], self.entry)
        manifest = json.loads((self.entry / 'manifest.json').read_text())
        self.assertFalse(manifest['shared_docker'])
        private_smoke=(self.entry/'spark-iceberg-v3-smoke.sql').read_text()
        self.assertTrue(private_smoke.startswith('CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_v3;'))
        self.assertNotIn('__NOVA_SMOKE',private_smoke)
        self.assertNotIn('DROP NAMESPACE',private_smoke)
        self.assertEqual(manifest['runtime']['profile'], 'stock')
        self.assertEqual(manifest['compose_file'], str(self.entry / 'compose.yml'))
        self.assertEqual(manifest['runtime']['template_model_sha256'], entry.model_hash(HERE.parent.parent))
        self.assertEqual(manifest['minio']['access_key_id'], self.config['credentials']['access_key'])
        self.assertIsNone(manifest['runtime']['control_uri'])
        model = (self.entry / 'compose.yml').read_text()
        for service in ('minio', 'mc-init', 'mc', 'rest', 'rest-mv', 'spark'):
            self.assertIn('  ' + service + ':\n', model)
        self.assertIn('mc mb --ignore-existing store/warehouse', model)
        self.assertIn('rest-catalog:/tmp', model)
        self.assertIn('rest-mv-catalog:/tmp', model)
        record = result['record']
        self.assertEqual(record['images']['rest-mv']['image_id'], 'sha256:rest-mv')
        self.assertIn('rest-mv', record['required_services'])
        self.assertIn('nr-isolated-rest-test_rest-mv-catalog', record['volumes'])
        self.assertEqual(record['service_ports']['rest-mv'], {'8181/tcp': 24101})
        self.assertIn('http://127.0.0.1:24101/v1/config', record['health_urls'])
        self.assertEqual(manifest['iceberg_rest_mv'], {'uri': 'http://127.0.0.1:24101',
            'warehouse': 's3://warehouse/test-worktree/rest-mv', 'server_default_warehouse': 's3://warehouse/test-worktree/rest-mv'})
        self.assertIn("NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI='s3://warehouse/test-worktree/rest-mv'", (self.entry / 'compose.env').read_text())
        self.assertEqual(os.readlink(sentinel), 'existing-worktree')
        hook = self.isolated('publication-hook')
        entry.render_entry(hook['context'], self.entry)
        manifest = json.loads((self.entry / 'manifest.json').read_text())
        self.assertEqual(manifest['runtime']['profile'], 'publication-hook')
        self.assertEqual(manifest['runtime']['control_uri'], 'http://127.0.0.1:23101')
        self.assertIn('8182', (self.entry / 'compose.yml').read_text())
        self.assertIn('TracingFileIO', (self.entry / 'compose.yml').read_text())
        self.assertIn('REST_IMAGE=\'publication:exact\'', (self.entry / 'compose.env').read_text())
        self.assertEqual(hook['record']['images']['rest-mv'], record['images']['rest-mv'])
        self.assertEqual(entry.blocks(entry.sections(model)['services'])['rest-mv'],
                         entry.blocks(entry.sections((self.entry / 'compose.yml').read_text())['services'])['rest-mv'])
        self.assertEqual(hook['record']['service_ports']['rest-mv'], record['service_ports']['rest-mv'])
        self.assertEqual(os.readlink(sentinel), 'existing-worktree')

    def test_prepare_and_teardown_shells_leave_current_untouched_without_docker(self):
        import shutil
        copied = self.root / 'checkout'
        fixture = copied / 'docker/iceberg-rest'
        fixture.mkdir(parents=True)
        for name in ['up.sh', 'down.sh', 'fixture_runtime.py', 'machine_runtime.py', 'entry_publication.py', 'runtime_entry.py', 'shared.env']:
            shutil.copy2(HERE / name, fixture / name)
        for name in ['templates', 'spark']:
            shutil.copytree(HERE / name, fixture / name)
        workspace = self.root / 'isolated-rest-request'
        workspace.mkdir()
        config = workspace / 'isolated.env'
        project = 'nr-isolated-rest-request'
        config.write_text('NOVA_ENV_SHARED_DOCKER=false\nNOVA_ENV_COMPOSE_PROJECT=' + project + '\nMINIO_ROOT_USER=isolated\nMINIO_ROOT_PASSWORD=isolated-secret\n')
        current = fixture / 'runtime/current'
        current.parent.mkdir(); current.symlink_to('untouched-worktree')
        fakebin = self.root / 'bin'; fakebin.mkdir()
        docker = fakebin / 'docker'; docker.write_text('#!/bin/sh\nexit 99\n'); docker.chmod(0o755)
        env = {**os.environ, 'PATH': str(fakebin) + ':' + os.environ['PATH'],
               'NOVAROCKS_WORKSPACE_ROOT': str(workspace), 'NOVA_ENV_CONFIG_FILE': str(config),
               'NOVA_ENV_SHARED_DOCKER': 'false', 'NOVA_ENV_COMPOSE_PROJECT': project,
               'NOVA_ENV_UPDATE_CURRENT': 'false', 'NOVA_ENV_ALLOW_VOLUME_DELETE': 'true',
               'NOVA_ENV_EXPECTED_COMPOSE_PROJECT': project, 'NOVA_ENV_EXPECTED_MINIO_VOLUME': project + '_minio-data'}
        prepared = subprocess.run(['bash', str(fixture / 'up.sh'), '--prepare-only'], env=env, capture_output=True, text=True)
        self.assertEqual(prepared.returncode, 0, prepared.stderr)
        generated = Path(json.loads(prepared.stdout)['published_dir'])
        self.assertEqual(generated.parent, fixture / 'runtime')
        self.assertFalse((workspace / 'docker' / 'iceberg-rest' / 'runtime').exists())
        self.assertEqual(os.readlink(current), 'untouched-worktree')
        env['NOVA_ENV_ID'] = generated.name
        shutil.rmtree(workspace)
        for _ in range(2):
            down = subprocess.run(['bash', str(fixture / 'down.sh'), '--docker', '--purge'], env=env, capture_output=True, text=True)
            self.assertEqual(down.returncode, 0, down.stderr)
        self.assertFalse(generated.exists())
        self.assertEqual(os.readlink(current), 'untouched-worktree')

    def test_hook_requires_explicit_nonzero_control_port(self):
        config = {**self.config, 'shared_docker': False}
        with self.assertRaises(runtime.RuntimeFailure):
            entry.render_isolated_stack(bom(), 'nr-isolated-rest-test', {'control': 0}, self.entry, config, profile='publication-hook', hook_image='exact')

    def test_isolated_teardown_is_exact_and_survives_missing_workspace(self):
        result = self.isolated()
        entry.render_entry(result['context'], self.entry)
        calls = []
        backend = mock.Mock()
        backend.compose.side_effect = lambda record, args: calls.append((record['project'], args))
        with mock.patch.object(runtime, 'Docker', return_value=backend):
            with self.assertRaises(runtime.RuntimeFailure):
                entry.isolated_down(self.entry, 'nr-isolated-rest-test', volumes=True, purge=True)
            self.assertTrue(self.entry.exists())
            with mock.patch.dict(os.environ, {'NOVA_ENV_ALLOW_VOLUME_DELETE': 'true', 'NOVA_ENV_EXPECTED_COMPOSE_PROJECT': 'nr-isolated-rest-test', 'NOVA_ENV_EXPECTED_MINIO_VOLUME': 'nr-isolated-rest-test_minio-data'}):
                self.workspace.rmdir()
                entry.isolated_down(self.entry, 'nr-isolated-rest-test', volumes=True, purge=True)
                entry.isolated_down(self.entry, 'nr-isolated-rest-test', volumes=True, purge=True)
        self.assertEqual(calls, [('nr-isolated-rest-test', ['down', '-v'])])
        self.assertFalse(self.entry.exists())




class ConsumerInputVerificationTests(unittest.TestCase):
    def test_runtime_consumes_exact_verified_snapshot_without_rereading_bom(self):
        snapshot = {'lock_sha256': 'verified-lock'}
        config = {'repo_root': '/fixture-repository', 'fixture_inputs': {'bom': '/fixture-store/bom.json', 'verified': False}}
        with mock.patch.object(machine, 'verified_bom', return_value=snapshot) as verify, mock.patch.object(runtime, 'read_json', side_effect=AssertionError('reread mutable BOM')):
            actual = entry.verify_inputs(config)
        self.assertIs(actual, snapshot)
        verify.assert_called_once_with(Path('/fixture-repository'), Path('/fixture-store'))
        self.assertEqual(config['fixture_inputs']['consumer'], 'iceberg-rest')
        self.assertEqual(config['fixture_inputs']['lock_sha256'], 'verified-lock')
        self.assertTrue(config['fixture_inputs']['verified'])

    def test_failed_input_verification_never_marks_publication_verified(self):
        config = {'repo_root': '/fixture-repository', 'fixture_inputs': {'bom': '/fixture-store/bom.json', 'verified': False}}
        with mock.patch.object(machine, 'verified_bom', side_effect=runtime.RuntimeFailure('FixturePrerequisiteMissing', 'absent')), self.assertRaises(runtime.RuntimeFailure):
            entry.verify_inputs(config)
        self.assertFalse(config['fixture_inputs']['verified'])


if __name__ == '__main__':
    unittest.main()
