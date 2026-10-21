# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
import json
import contextlib
import io
from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_run
import fixture_runtime


class RunTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.env = self.root / "env.sh"
        self.manifest = {"ready": True, "runtime": {"machine": {
            "containers": {"spark": "exact-spark"},
            "policy": {"max_run_seconds": 86400, "cleanup_grace_seconds": 300,
                       "scratch_ttl_seconds": 604800}}}}
        self.write_manifest()
        self.env.write_text("export NOVA_ENV_ID='worktree-id'\n")

    def write_manifest(self):
        (self.root / "manifest.json").write_text(json.dumps(self.manifest))

    def sweep_input(self):
        self.manifest["runtime"]["machine"].update(control_root=str(self.root/"machine"), daemon_id="exact-daemon", version="exact-version", generation="exact-generation")
        self.write_manifest()
        return fixture_run.admit(self.env)[2]

    def test_explicit_startup_sweep_keeps_original_clock_and_pins_machine_identity(self):
        with patch.dict(os.environ,{},clear=True):
            budget=self.sweep_input(); original=budget.as_dict()
            budget=replace(budget,deadline=time.monotonic()+budget.cleanup_grace_seconds+2)
            before=time.monotonic()
            with patch("machine_runtime.MachineOwner") as owner, patch.object(fixture_run.ScratchIdentity,"create",side_effect=AssertionError("must inherit original identity")):
                owner.return_value.sweep.return_value={"complete":True,"removed":[],"errors":[],"observed_counts":{"s3-prefix":3}}
                report=fixture_run.startup_sweep(self.env,budget=budget)
            self.assertEqual(report["observed_counts"],{"s3-prefix":3})
            self.assertTrue(report["complete"]); self.assertFalse(report["pending"])
            self.assertEqual(budget.as_dict(),original)
            self.assertEqual(owner.call_args.args,(str(self.root/"machine"),))
            self.assertEqual(owner.call_args.kwargs["daemon"],"exact-daemon")
            kwargs=owner.return_value.sweep.call_args.kwargs
            self.assertEqual(kwargs["expected_version"],"exact-version")
            self.assertEqual(kwargs["expected_generation"],"exact-generation")
            self.assertLessEqual(kwargs["deadline"],before+2.05)
            self.assertGreater(kwargs["deadline"],before)

    def test_startup_sweep_allows_only_bounded_native_scratch_timeout_as_partial(self):
        with patch.dict(os.environ,{},clear=True):
            budget=self.sweep_input()
            for value in ({"complete":False,"removed":[],"errors":[{"code":"ScratchSweepTimeout"}]}, fixture_runtime.RuntimeFailure("ScratchSweepTimeout","bounded cleanup settled")):
                with patch("machine_runtime.MachineOwner") as owner:
                    if isinstance(value,Exception): owner.return_value.sweep.side_effect=value
                    else: owner.return_value.sweep.return_value=value
                    report=fixture_run.startup_sweep(self.env,budget=budget)
                self.assertFalse(report["complete"]); self.assertTrue(report["pending"])
                self.assertEqual(report["errors"][0]["code"],"ScratchSweepTimeout")

    def test_startup_sweep_never_hides_io_cleanup_or_unclassified_partial_errors(self):
        with patch.dict(os.environ,{},clear=True):
            budget=self.sweep_input()
            for code in ("ScratchAccessDenied","ContainerCleanupFailed","DockerOperationTimeout","ScratchSweepTraversalLimit"):
                report={"complete":False,"removed":[],"errors":[{"code":code},{"code":"ScratchSweepTimeout"}]}
                with patch("machine_runtime.MachineOwner") as owner:
                    owner.return_value.sweep.return_value=report
                    with self.assertRaises(fixture_runtime.RuntimeFailure) as caught: fixture_run.startup_sweep(self.env,budget=budget)
                self.assertEqual(caught.exception.code,code)
                self.assertEqual(caught.exception.report["errors"],report["errors"])
            with patch("machine_runtime.MachineOwner") as owner:
                owner.return_value.sweep.side_effect=fixture_runtime.RuntimeFailure("MachineVersionMismatch","original snapshot changed")
                with self.assertRaises(fixture_runtime.RuntimeFailure) as caught: fixture_run.startup_sweep(self.env,budget=budget)
                self.assertEqual(caught.exception.code,"MachineVersionMismatch")
                self.assertEqual(caught.exception.report["errors"][0]["code"],"MachineVersionMismatch")
            with patch("machine_runtime.MachineOwner") as owner:
                owner.return_value.sweep.side_effect=OSError("private native diagnostics")
                with self.assertRaises(fixture_runtime.RuntimeFailure) as caught: fixture_run.startup_sweep(self.env,budget=budget)
                self.assertEqual(caught.exception.code,"ScratchSweepIOFailed")
                self.assertNotIn("private native diagnostics",str(caught.exception))
                self.assertTrue(caught.exception.report["observations_unavailable"])

    def test_startup_sweep_expiration_is_run_failure_before_or_after_sweep(self):
        with patch.dict(os.environ,{},clear=True):
            budget=self.sweep_input()
            budget=replace(budget,deadline=time.monotonic()+budget.cleanup_grace_seconds-0.1)
            with patch("machine_runtime.MachineOwner") as owner:
                with self.assertRaisesRegex(fixture_runtime.RuntimeFailure,"RunBudgetExpired"): fixture_run.startup_sweep(self.env,budget=budget)
                owner.assert_not_called()
            clock=[time.monotonic()]
            budget=replace(budget,deadline=clock[0]+budget.cleanup_grace_seconds+2,monotonic=lambda:clock[0])
            def expire(**kwargs):
                clock[0]+=3
                return {"complete":True,"removed":[],"errors":[]}
            with patch("machine_runtime.MachineOwner") as owner:
                owner.return_value.sweep.side_effect=expire
                with self.assertRaisesRegex(fixture_runtime.RuntimeFailure,"RunBudgetExpired") as caught: fixture_run.startup_sweep(self.env,budget=budget)
            self.assertEqual(caught.exception.report,{"complete":True,"removed":[],"errors":[]})

    def test_generic_admission_and_context_remain_read_only_and_cli_retains_failed_report(self):
        with patch.dict(os.environ,{},clear=True):
            budget=self.sweep_input()
            os.environ["NOVA_FIXTURE_RUN_CONTEXT"]=json.dumps(budget.as_dict())
            with patch("machine_runtime.MachineOwner") as owner, patch.object(fixture_runtime,"install_signal_handlers"), contextlib.redirect_stdout(io.StringIO()) as output:
                fixture_run.admit(self.env)
                self.assertEqual(fixture_run.main(["context","--env-file",str(self.env)]),0)
                owner.assert_not_called()
            self.assertEqual(json.loads(output.getvalue()),budget.as_dict())
            with patch("machine_runtime.MachineOwner") as owner, patch.object(fixture_runtime,"install_signal_handlers"), contextlib.redirect_stdout(io.StringIO()) as output:
                owner.return_value.sweep.return_value={"complete":False,"errors":[{"code":"ScratchAccessDenied"}],"removed":[]}
                with self.assertRaises(fixture_runtime.RuntimeFailure): fixture_run.main(["sweep-start","--env-file",str(self.env)])
            self.assertEqual(json.loads(output.getvalue())["errors"][0]["code"],"ScratchAccessDenied")
            self.manifest["runtime"]={"catalog":{"id":"private"}}; self.write_manifest()
            with patch("machine_runtime.MachineOwner") as owner:
                self.assertTrue(fixture_run.startup_sweep(self.env)["skipped"])
                owner.assert_not_called()

    def test_copy_targets_captured_spark_in_shared_and_hybrid_publications(self):
        for shared in (True, False):
            self.manifest["shared_docker"] = shared
            self.manifest["runtime"]["catalog"] = {"containers": {"spark": "private-exact"}}
            self.write_manifest()
            with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate") as run:
                fixture_run.compose(self.env, ["-p", "published", "cp", "--archive", "input", "spark:/tmp/output"])
            self.assertEqual(run.call_args.args[0], ["docker", "cp", "--archive", "input",
                ("exact-spark" if shared else "private-exact") + ":/tmp/output"])

    def test_persisted_context_keeps_deadline_and_refuses_replacement(self):
        with patch.dict(os.environ, {}, clear=True):
            original = fixture_run.admit(self.env)[2].as_dict()
            saved = self.root / "state.json"
            saved.write_text(json.dumps({"run_context": original}))
            self.assertTrue(fixture_run.restore_context(saved))
            self.assertEqual(fixture_run.admit(self.env)[2].as_dict(), original)
            replacement = fixture_run.admit(self.env, inherited={**original, "deadline_unix_ms": original["deadline_unix_ms"] - 1000})[2]
            os.environ["NOVA_FIXTURE_RUN_CONTEXT"] = json.dumps(replacement.as_dict())
            with self.assertRaisesRegex(fixture_runtime.RuntimeFailure, "persisted original"):
                fixture_run.restore_context(saved)
            original["started_unix_ms"] -= 10000000
            original["deadline_unix_ms"] -= 10000000
            saved.write_text(json.dumps({"run_context": original}))
            os.environ.pop("NOVA_FIXTURE_RUN_CONTEXT")
            fixture_run.restore_context(saved)
            with self.assertRaises(fixture_runtime.RuntimeFailure):
                fixture_run.admit(self.env)

    def test_temp_marker_cleanup_uses_original_reserve_and_native_status(self):
        self.env.write_text("export NOVA_ENV_ID=worktree-id\nexport NOVA_ENV_COMPOSE_ENV=/published/compose.env\nexport NOVA_ENV_COMPOSE_PROJECT=published\nexport NOVA_ENV_COMPOSE_FILE=/published/compose.yml\n")
        with patch.dict(os.environ, {}, clear=True):
            original = fixture_run.admit(self.env)[2].as_dict()
            original["started_unix_ms"] -= 3400000
            original["deadline_unix_ms"] -= 3400000
            os.environ["NOVA_FIXTURE_RUN_CONTEXT"] = json.dumps(original)
            with self.assertRaises(fixture_runtime.RuntimeFailure):
                fixture_run.admit(self.env)
            os.environ["NOVA_FIXTURE_CLEANUP_MODE"] = "true"
            folder = "/tmp/novarocks-fixture-data-" + original["identity"]["run_id"] + "-" + "a" * 32
            with patch.object(fixture_run, "compose", return_value=subprocess.CompletedProcess([], 0)) as run:
                fixture_run.cleanup_temp(self.env, folder)
                marker = json.loads(run.call_args.args[1][-1])
                self.assertEqual(marker["context"], original)
                self.assertIn("marker.read_text()", run.call_args.args[1][-3])
            with patch.object(fixture_run, "compose", return_value=subprocess.CompletedProcess([], 13)):
                with self.assertRaisesRegex(fixture_runtime.RuntimeFailure, "removal exited 13"):
                    fixture_run.cleanup_temp(self.env, folder)
            with patch.object(fixture_run, "compose") as run:
                with self.assertRaises(fixture_runtime.RuntimeFailure):
                    fixture_run.cleanup_temp(self.env, folder.replace(original["identity"]["run_id"], "foreign"))
                run.assert_not_called()

    def test_nested_budget_keeps_original_identity_and_deadline(self):
        with patch.dict(os.environ, {}, clear=True):
            first = fixture_run.admit(self.env)[2]
            nested = fixture_run.admit(self.env, inherited=first.as_dict())[2]
        self.assertEqual(first.as_dict(), nested.as_dict())
        self.assertLessEqual(nested.remaining_seconds(), first.remaining_seconds() + 0.01)

    def test_overlarge_budget_is_rejected_before_docker(self):
        with patch.object(fixture_run, "communicate") as run:
            for seconds in (0, -1, 86401):
                with self.subTest(seconds=seconds), self.assertRaises(fixture_runtime.RuntimeFailure):
                    fixture_run.admit(self.env, seconds)
        run.assert_not_called()

    def test_private_publication_does_not_gain_shared_identity(self):
        self.manifest["runtime"] = {"catalog": {"id": "private"}}
        self.write_manifest()
        self.assertIsNone(fixture_run.admit(self.env)[2])

    def test_shell_values_are_decoded_without_execution(self):
        self.env.write_text("export NOVA_ENV_ID='$(exit 9)'\nunset OLD\n")
        self.assertEqual(fixture_run.env_values(self.env)["NOVA_ENV_ID"], "$(exit 9)")

    def test_spark_exec_preserves_flags_and_confirms_actual_wait(self):
        calls = []
        def run(command, **kwargs):
            calls.append(command)
            output = b'{"child_waited":true,"returncode":0}' if fixture_run.CONTAINER_SETTLE in command else (b"exact-spark" if command[-3:] == ["ps", "-q", "spark"] else b"query-output")
            return subprocess.CompletedProcess(command, 0, output, b"")
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate", side_effect=run):
            result = fixture_run.compose(self.env, ["-p", "fixed", "exec", "-T", "-e", "FLAG=1", "spark", "bash", "-lc", "echo hi"], capture=True)
        self.assertEqual(result.stdout, b"query-output")
        self.assertIn("FLAG=1", calls[1])
        self.assertEqual(calls[1][-3:], ["bash", "-lc", "echo hi"])
        self.assertIn(fixture_run.CONTAINER_SETTLE, calls[2])
        self.assertEqual(calls[-1][-3:-1], ["rm", "-rf"])

    def test_failed_worker_cleanup_is_not_reported_as_success(self):
        responses = [subprocess.CompletedProcess([], 0, b"exact-spark", b""),
                     subprocess.CompletedProcess([], 0, b"ok", b""),
                     subprocess.CompletedProcess([], 125, b"", b"")]
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate", side_effect=responses):
            with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                fixture_run.compose(self.env, ["exec", "-T", "spark", "true"], capture=True)
        self.assertEqual(caught.exception.code, "ContainerCleanupFailed")

    def test_replaced_spark_is_rejected_before_exec(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate",
                return_value=subprocess.CompletedProcess([], 0, b"replacement-spark", b"")) as run:
            with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                fixture_run.compose(self.env, ["exec", "-T", "spark", "true"])
        self.assertEqual(caught.exception.code, "MachineUnavailable")
        self.assertEqual(run.call_count, 1)
        self.assertEqual(run.call_args.args[0][-3:], ["ps", "-q", "spark"])

    def test_private_spark_with_shared_minio_uses_its_own_exact_container(self):
        self.manifest["shared_docker"] = False
        self.manifest["runtime"]["catalog"] = {"containers": {"spark": "private-spark"}}
        self.write_manifest()
        calls = []
        def run(command, **kwargs):
            calls.append(command)
            output = b"private-spark" if command[-3:] == ["ps", "-q", "spark"] else b'{"returncode":0,"child_waited":true}'
            return subprocess.CompletedProcess(command, 0, output, b"")
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate", side_effect=run):
            fixture_run.compose(self.env, ["exec", "-T", "spark", "true"])
        for command in calls[1:]:
            self.assertIn("private-spark", command)
            self.assertNotIn("exact-spark", command)

    def test_successful_cli_cannot_hide_failed_worker_or_tmp_cleanup(self):
        cases = [({"returncode": 9, "child_waited": True}, 0),
                 ({"returncode": 0, "child_waited": False}, 0),
                 ({"returncode": 0, "child_waited": True}, 8)]
        for receipt, cleanup_code in cases:
            with self.subTest(receipt=receipt, cleanup_code=cleanup_code):
                responses = [subprocess.CompletedProcess([], 0, b"exact-spark", b""),
                             subprocess.CompletedProcess([], 0, b"", b""),
                             subprocess.CompletedProcess([], 0, json.dumps(receipt).encode(), b""),
                             subprocess.CompletedProcess([], cleanup_code, b"", b"")]
                with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate", side_effect=responses):
                    with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                        fixture_run.compose(self.env, ["exec", "-T", "spark", "true"])
                self.assertEqual(caught.exception.code, "ContainerCleanupFailed")

    @unittest.skipUnless(sys.platform.startswith("linux"), "worker runs inside the Linux Spark image")
    def test_worker_retains_actual_exit_and_removes_a_timed_out_process(self):
        for script, grace, expected in (("raise SystemExit(17)", 0, 17),
                                        ("import time; time.sleep(30)", 0.2, 124)):
            with self.subTest(expected=expected):
                folder = self.root / str(expected)
                result = subprocess.run([sys.executable, "-c", fixture_run.CONTAINER_OWNER,
                    str(folder), str(time.time() + 0.5), str(grace), "{}",
                    sys.executable, "-c", script], timeout=4)
                self.assertEqual(result.returncode, expected)
                self.assertEqual(json.loads((folder / "result.json").read_text()),
                                 {"returncode": expected, "child_waited": True})

    def test_expired_settlement_retains_owner_until_observed_group_exit(self):
        folder=self.root/"late-settlement"
        release=self.root/"release-group"
        proc=self.root/"fake-proc"; proc.mkdir()
        entry=proc/"1"; entry.mkdir()
        # Exercise the actual owner/waitid state machine on this OS. Only
        # Linux /proc group observation and killpg are simulated here.
        observation = "\n".join([
            "nr_original_iterdir=pathlib.Path.iterdir",
            "def nr_observation(path):",
            "    if str(path) != '/proc': return nr_original_iterdir(path)",
            "    if pathlib.Path("+repr(str(release))+").exists(): return iter([])",
            "    entry=pathlib.Path("+repr(str(entry))+")",
            "    (entry/'stat').write_text('1 (simulated descendant) S 0 '+str(child.pid)+' 0')",
            "    return iter([entry])",
            "pathlib.Path.iterdir=nr_observation",
            "os.killpg=lambda pid,number: None",
        ])
        code=fixture_run.CONTAINER_OWNER.replace('pathlib.Path("/tmp")','pathlib.Path('+repr(str(self.root))+')')
        code=code.replace('# Keep the leader unreaped',observation+'\n# Keep the leader unreaped')
        worker=subprocess.Popen([sys.executable,"-c",code,str(folder),str(time.time()+0.3),"0","{}",
                                 sys.executable,"-c","raise SystemExit(17)"],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            end=time.monotonic()+3
            while time.monotonic()<end and not (folder/"result.json").exists(): time.sleep(0.01)
            fact=json.loads((folder/"result.json").read_text())
            self.assertFalse(fact["child_waited"])
            self.assertTrue(fact["cleanup_failed"])
            self.assertTrue(fact["cleanup_deadline_expired"])
            self.assertIsNone(worker.poll(),"owner must retain the unreaped leader past failed deadline")
            release.touch()
            _,error=worker.communicate(timeout=3)
            self.assertEqual(worker.returncode,125,error)
            final=json.loads((folder/"result.json").read_text())
            self.assertTrue(final["child_waited"])
            self.assertTrue(final["cleanup_failed"],"late actual exit must not erase deadline failure")
        finally:
            release.touch()
            if worker.poll() is None: worker.kill()
            worker.communicate()

    def test_cancellation_tombstone_prevents_a_late_worker_start(self):
        folder = self.root / "late"
        marker = self.root / "must-not-exist"
        stopped = subprocess.run([sys.executable, "-c", fixture_run.CONTAINER_SETTLE,
                                  str(folder), str(time.time() + 1), "{}"], capture_output=True)
        self.assertEqual(stopped.returncode, 0)
        late = subprocess.run([sys.executable, "-c", fixture_run.CONTAINER_OWNER,
            str(folder), str(time.time() + 1), "0", "{}", sys.executable, "-c",
            "from pathlib import Path; Path(" + repr(str(marker)) + ").touch()"],
            capture_output=True)
        self.assertNotEqual(late.returncode, 0)
        self.assertFalse(marker.exists())

    def test_expired_temp_gc_requires_exact_private_marker_and_settled_run(self):
        identity = fixture_run.ScratchIdentity.create("s3://novarocks/test", now_ms=int(time.time() * 1000) - 10000)
        context = {"schema": 1, "identity": identity.as_dict(),
                   "started_unix_ms": identity.as_dict()["created_unix_ms"],
                   "deadline_unix_ms": int(time.time() * 1000) - 5000}
        folders = []
        for suffix, owner in (("1" * 32, "novarocks-fixture-run-v1"), ("2" * 32, "foreign")):
            folder = self.root / ("novarocks-fixture-data-" + identity.run_id + "-" + suffix)
            folder.mkdir(mode=0o700)
            (folder / ".fixture-context.json").write_text(json.dumps({"schema": 1, "owner": owner, "context": context}))
            folders.append(folder)
        unsettled = fixture_run.ScratchIdentity.create("s3://novarocks/test", now_ms=int(time.time() * 1000) - 10000,
                                                       token="3" * 32)
        unsafe_context = {**context, "identity": unsettled.as_dict()}
        retained = []
        for kind in ("data", "run"):
            folder = self.root / ("novarocks-fixture-" + kind + "-" + unsettled.run_id + "-" + "4" * 32)
            folder.mkdir(mode=0o700)
            (folder / ".fixture-context.json").write_text(json.dumps({"schema": 1, "owner": "novarocks-fixture-run-v1", "context": unsafe_context}))
            if kind == "run":
                (folder / "result.json").write_text('{"child_waited":false,"cleanup_failed":true}')
            retained.append(folder)
        # Remap only the temporary filesystem root for the hermetic carrier;
        # execute the actual classifier and leave the host's /tmp untouched.
        code = fixture_run.CONTAINER_OWNER.replace('pathlib.Path("/tmp")', 'pathlib.Path(' + repr(str(self.root)) + ')')
        result = subprocess.run([sys.executable, "-c", code, str(self.root / "expired-owner"),
            str(time.time() - 1), "0", "{}", "must-not-start"], capture_output=True)
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertFalse(folders[0].exists())
        self.assertTrue(folders[1].exists())
        self.assertTrue(all(folder.exists() for folder in retained))


if __name__ == "__main__":
    unittest.main()
