# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""RF publication scope/budget checks without external services."""
import io
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).resolve().parent))
import publish
import publish_whole


class PublicationBindingTests(unittest.TestCase):
    def setUp(self):
        sweep = patch.object(publish.fixture_run, "startup_sweep", return_value={"complete": True})
        self.sweep = sweep.start()
        self.addCleanup(sweep.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.env = self.root / "env.sh"
        self.env.write_text("export NOVA_ENV_ID=worktree\n")
        manifest = {"ready":True,"runtime":{"machine":{"id":"machine-test","control_root":"/owned/machine","daemon_id":"daemon-test","version":"version-test","generation":"generation-test","config":{"minio_volume":"volume-test"},"server_warehouses":{"rest":"s3://warehouse/machine/rest","rest-mv":"s3://warehouse/machine/rest-mv"},"policy":{
            "max_run_seconds":86400,"cleanup_grace_seconds":300,"scratch_ttl_seconds":604800}}}}
        self.env.with_name("manifest.json").write_text(json.dumps(manifest))
        self.addCleanup(lambda: signal.setitimer(signal.ITIMER_REAL,0))
        self.addCleanup(setattr,publish,"RUN",None)

    def test_shared_namespace_and_every_table_location_belong_to_original_run(self):
        captured=[]
        class Catalog:
            def namespace_exists(inner,namespace): return False
            def create_namespace(inner,namespace,properties): captured.append((namespace,properties))
            def create_table(inner,identifier,**kwargs):
                captured.append((identifier,kwargs))
                table=Mock()
                table.location.return_value=kwargs["location"]
                table.io.new_output.return_value.create.return_value=io.BytesIO()
                return table
        with patch.dict(os.environ,{"NOVA_ENV_REST_ENV_FILE":str(self.env),
                "NOVAROCKS_ICEBERG_REST_URI":"http://saved","NOVAROCKS_ICEBERG_REST_WAREHOUSE":"saved"},clear=True):
            run=publish.admit_run()
            original=run.as_dict()
            namespace=publish.writer_namespace(None)
            data=self.root/"data"; data.mkdir()
            for filename,_ in publish.TABLES.values(): (data/filename).write_bytes(b"fixture")
            (data/publish.MANIFEST_FILE).write_text("manifest")
            with patch.object(publish,"verify"),patch.object(publish,"catalog_from_environment",return_value=Catalog()), \
                    patch.object(publish,"published_table_record",side_effect=lambda c,n,t,p:{"table":t,"snapshot_id":1}):
                publish.publish(data,self.root/"new.json",namespace)
            receipt=json.loads((self.root/"new.json").read_text())
            self.assertEqual(receipt["run_context"],original)
            self.assertEqual(captured[0],(namespace,{}))
            for identifier,request in captured[1:]:
                self.assertEqual(request["location"],run.identity.scope_root+"/rest/"+namespace+"/"+identifier[1])
            with self.assertRaisesRegex(ValueError,"original scratch"):
                publish.writer_namespace("frozen_foreign_namespace")
            nested=publish.admit_run()
            self.assertEqual(nested.as_dict(),original)
            self.sweep.assert_called_once_with(self.env, budget=run)

    def test_whole_publish_restores_source_context_and_expired_run_cannot_dispatch(self):
        with patch.dict(os.environ,{"NOVA_ENV_REST_ENV_FILE":str(self.env),
                "NOVAROCKS_ICEBERG_REST_URI":"saved","NOVAROCKS_ICEBERG_REST_WAREHOUSE":"warehouse"},clear=True):
            run=publish.admit_run()
            source=self.root/"source.json"
            original=run.as_dict()
            source.write_text(json.dumps({"run_context":original,"namespace":run.identity.resource_name("rf"),
                "rest_uri":"saved","warehouse":"warehouse"}))
            os.environ.pop("NOVA_FIXTURE_RUN_CONTEXT")
            with patch.object(sys,"argv",["publish_whole.py","publish","--source-receipt",str(source)]), \
                    patch.object(publish_whole,"publish") as write:
                publish_whole.main()
            self.assertEqual(write.call_args.args[2],run.identity.resource_name("rf"))
            self.assertEqual(publish.RUN.as_dict(),original)
            original["started_unix_ms"]-=10000000
            original["deadline_unix_ms"]-=10000000
            source.write_text(json.dumps({"run_context":original}))
            os.environ.pop("NOVA_FIXTURE_RUN_CONTEXT")
            with patch.object(sys,"argv",["publish_whole.py","publish","--source-receipt",str(source)]), \
                    patch.object(publish_whole,"publish") as write:
                with self.assertRaises(publish.fixture_run.runtime.RuntimeFailure): publish_whole.main()
                write.assert_not_called()

    def test_private_whole_publish_keeps_original_namespace_without_source_receipt(self):
        with patch.dict(os.environ,{},clear=True), \
                patch.object(sys,"argv",["publish_whole.py","publish","--source-receipt",str(self.root/"absent.json"),"--namespace","private_original"]), \
                patch.object(publish,"admit_run",return_value=None), \
                patch.object(publish_whole,"publish") as write:
            publish_whole.main()
        self.assertEqual(write.call_args.args[2],"private_original")

    def test_whole_verify_keeps_the_frozen_receipt_namespace_read_only(self):
        with patch.dict(os.environ,{"NOVA_ENV_REST_ENV_FILE":str(self.env)},clear=True), \
                patch.object(sys,"argv",["publish_whole.py","verify-published"]), \
                patch.object(publish_whole,"verify_published") as verify,patch.object(publish_whole,"publish") as write:
            publish_whole.main()
        verify.assert_called_once()
        write.assert_not_called()
        self.sweep.assert_not_called()

    def test_whole_publish_sweeps_original_budget_before_create_table(self):
        with patch.dict(os.environ, {"NOVA_ENV_REST_ENV_FILE":str(self.env)}, clear=True):
            run = publish.admit_run()
            namespace = run.identity.resource_name("rf")
            catalog = Mock()
            catalog.namespace_exists.return_value = True
            catalog.table_exists.return_value = False
            catalog.load_table.return_value.location.return_value = run.identity.scope_root + "/rest/" + namespace + "/probe_v1"
            def create(*args, **kwargs):
                self.sweep.assert_called_once_with(self.env, budget=run)
                raise RuntimeError("stop before table creation")
            catalog.create_table.side_effect = create
            with patch.object(publish_whole, "verify"), \
                    patch.object(publish_whole, "catalog_from_environment", return_value=catalog), \
                    patch.object(publish_whole, "source_record", return_value={}):
                with self.assertRaisesRegex(RuntimeError, "stop before table creation"):
                    publish_whole.publish(self.root, self.root / "new-receipt.json", namespace)


if __name__=="__main__": unittest.main()
