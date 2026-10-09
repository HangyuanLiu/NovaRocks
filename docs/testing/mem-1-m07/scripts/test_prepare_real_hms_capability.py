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
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Local host-only boundary checks; no provider/fixture or Docker process."""
import importlib.util
import os
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

path=Path(__file__).with_name('prepare_real_hms_capability.py')
spec=importlib.util.spec_from_file_location('m07_hms_capture_v5',path)
helper=importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)

class CaptureBoundary(unittest.TestCase):
    def capture(self,code,ownership='git-readonly',cap=1024,interrupt=False):
        children=[]
        args=[sys.executable,'-c',code]
        now=time.monotonic()
        if interrupt:
            with patch.object(helper.selectors.DefaultSelector,'select',side_effect=KeyboardInterrupt):
                return helper.capture(args,os.environ.copy(),now+2,cap,owned_children=children,
                    wall_deadline=now+8,reap_seconds=5,ownership=ownership),children
        return helper.capture(args,os.environ.copy(),now+2,cap,owned_children=children,
            wall_deadline=now+8,reap_seconds=5,ownership=ownership),children

    def assert_settled_host(self,facts):
        self.assertTrue(facts['leader_reaped'])
        self.assertTrue(facts['group_exit_confirmed'])
        with self.assertRaises(ProcessLookupError):
            os.killpg(facts['owned_group_id'],0)

    def test_normal_direct_capture_exits_without_kill(self):
        (code,output,facts),children=self.capture("print('bounded')")
        self.assertEqual(code,0)
        self.assertEqual(output,b'bounded\n')
        self.assertEqual(children,[])
        self.assert_settled_host(facts)
        self.assertEqual(facts['kill_outcome'],'not-authorized-after-leader-reap')
        self.assertFalse(facts['resource_retained_whole_failure'])

    def test_actual_child_output_overflow_retains_partial_reason_and_owner(self):
        try:
            self.capture("import sys,time; sys.stdout.write('x'*4096); sys.stdout.flush(); time.sleep(3)",
                ownership='owner-wrapper',cap=128)
        except helper.CaptureFailure as error:
            self.assertEqual(error.primary_reason,'child output exceeds frozen bound')
            self.assertEqual(error.output,b'x'*128)
            self.assert_settled_host(error.exit_facts)
            self.assertTrue(error.exit_facts['resource_retained_whole_failure'])
            self.assertFalse(error.exit_facts['detached_child_exit_confirmed'])
        else:
            self.fail('overflow unexpectedly passed')

    def test_keyboard_interrupt_after_popen_cannot_bypass_owner_retention(self):
        try:
            self.capture('import time; time.sleep(3)',ownership='owner-wrapper',interrupt=True)
        except helper.CaptureFailure as error:
            self.assert_settled_host(error.exit_facts)
            self.assertTrue(error.exit_facts['resource_retained_whole_failure'])
            self.assertFalse(error.exit_facts['detached_child_exit_confirmed'])
            self.assertTrue(error.exit_facts['cancel_observed'])
            self.assertEqual(error.safe_facts()['primary_exception_class'],'KeyboardInterrupt')
            self.assertEqual(error.output,b'')
        else:
            self.fail('interrupt escaped or unexpectedly passed')

if __name__=='__main__':
    unittest.main()
