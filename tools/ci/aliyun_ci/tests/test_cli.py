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

"""Offline commands stay usable without SDKs or credentials; identities round-trip."""
from contextlib import redirect_stdout
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from aliyun_ci import cli
from aliyun_ci.config import validate_config
from aliyun_ci.plan import derive
from .test_config import example


class CliTests(unittest.TestCase):
    def test_offline_commands_need_no_cloud_import_or_credentials(self):
        path = Path(__file__).parents[1] / 'config.example.json'
        for command in (['validate-config'], ['plan-run', '--run-id', '42', '--now', '2026-10-28T00:00:00Z', '--nonce', 'fixed'], ['deploy', 'render']):
            with self.subTest(command=command), redirect_stdout(io.StringIO()) as output:
                self.assertEqual(0, cli.main(['--config', str(path), *command]))
                result = json.loads(output.getvalue())
            self.assertIsInstance(result, dict)

    def test_saved_start_plan_keeps_original_absolute_deadline(self):
        config = validate_config(example())
        plan = derive(config, '42', 2, datetime(2026, 10, 28, tzinfo=timezone.utc))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'plan.json'
            path.write_text(json.dumps({'plan': plan.to_dict()}))
            args = cli.parser().parse_args(['--config', 'unused', 'teardown', '--plan', str(path)])
            self.assertEqual(plan, cli.load_plan(config, args))
            data = plan.to_dict(); data['tags']['deployment'] = 'another'
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, 'deployment'):
                cli.load_plan(config, args)
