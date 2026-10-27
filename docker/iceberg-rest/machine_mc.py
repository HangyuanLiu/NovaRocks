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

"""Run one private mc script under the original machine and caller deadlines."""
import argparse
import json
import os
from pathlib import Path
import sys

import fixture_runtime as runtime
import machine_runtime as machine
from scratch import RUN_STORAGE_ENV, RunBudget, RunStorageScope, ScratchIdentity, ScratchSweeper


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--daemon', required=True)
    parser.add_argument('--expected-version', required=True)
    parser.add_argument('--expected-generation', required=True)
    parser.add_argument('--context-file', type=Path)
    parser.add_argument('--script-file', type=Path)
    parser.add_argument('--cleanup', action='store_true')
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--cleanup-run', action='store_true')
    action.add_argument('--check-users-absent', action='store_true')
    action.add_argument('--cleanup-users', action='store_true')
    args = parser.parse_args(argv)
    runtime.install_signal_handlers()
    try:
        raw = args.context_file.read_text() if args.context_file else os.environ.get('NOVA_FIXTURE_RUN_CONTEXT', '')
        if len(raw) > 16384:
            raise runtime.RuntimeFailure('RunBudgetRejected', 'run context exceeds its input bound')
        context = json.loads(raw)
        identity = ScratchIdentity.from_dict(context.get('identity'))
        if args.check_users_absent and args.cleanup:
            raise runtime.RuntimeFailure('RunBudgetRejected', 'IAM absence check requires the original work phase')
        if args.cleanup_users and not args.cleanup:
            raise runtime.RuntimeFailure('RunBudgetRejected', 'IAM cleanup requires the original cleanup phase')
        owner = machine.MachineOwner(args.root, daemon=args.daemon)
        state = owner.read_saved_state()
        if not state:
            raise runtime.RuntimeFailure('MachineUnavailable', 'machine has no saved authority')
        budget = RunBudget.admit(state, identity, inherited=context, cleanup=args.cleanup)
        deadline = budget.deadline - (0 if args.cleanup else budget.cleanup_grace_seconds)
        if args.script_file:
            with args.script_file.open() as source:
                script = source.read(65537)
        else:
            script = sys.stdin.read(65537)
        if len(script) > 65536 or not script:
            raise runtime.RuntimeFailure('InvalidConfiguration', 'mc script is empty or exceeds its input bound')
        with owner.ready_snapshot({'version': args.expected_version}, deadline=deadline,
                                  expected_generation=args.expected_generation) as snapshot:
            inherited_storage = os.environ.get(RUN_STORAGE_ENV)
            if inherited_storage is not None:
                if len(inherited_storage) > 16384:
                    raise runtime.RuntimeFailure('RunBudgetRejected', 'run storage exceeds its input bound')
                RunStorageScope.from_dict(json.loads(inherited_storage)).validate_against(identity, snapshot)
            if args.cleanup_run or args.check_users_absent or args.cleanup_users:
                if args.cleanup_run and not args.cleanup:
                    raise runtime.RuntimeFailure('RunBudgetRejected', 'exact run cleanup requires the original cleanup phase')
                request = json.loads(script)
                if ((args.check_users_absent or args.cleanup_users)
                        and (not isinstance(request, dict) or set(request) != {'identity', 'storage', 'users'})):
                    raise runtime.RuntimeFailure('RunBudgetRejected', 'invalid exact IAM request shape')
                if ScratchIdentity.from_dict(request.get('identity')) != identity:
                    raise runtime.RuntimeFailure('RunBudgetRejected', 'cleanup identity differs from original context')
                scope = RunStorageScope.from_dict(request.get('storage'))
                scope.validate_against(identity, snapshot)
                sweeper = ScratchSweeper(snapshot, owner.backend)
                if args.check_users_absent:
                    report = sweeper.check_users_absent(scope, request.get('users'), deadline=deadline)
                elif args.cleanup_users:
                    report = sweeper.cleanup_users(scope, request.get('users'), deadline=deadline)
                else:
                    report = sweeper.cleanup_run(scope, request.get('users'), deadline=deadline)
                if not report.get('complete'):
                    sys.stderr.write(json.dumps(report) + '\n')
                    if args.check_users_absent:
                        raise runtime.RuntimeFailure('ScratchIamAbsenceUnconfirmed', 'exact IAM absence was not confirmed')
                    detail = 'exact IAM cleanup did not complete' if args.cleanup_users else 'exact run cleanup did not complete'
                    raise runtime.RuntimeFailure('ScratchCleanupIncomplete', detail)
                output = json.dumps(report) + '\n'
            else:
                output = owner.backend.run_mc(snapshot, script, deadline=deadline)
        sys.stdout.write(output)
        return 0
    except runtime.RuntimeFailure as error:
        print(error, file=sys.stderr)
        return 75 if error.code in machine.BLOCKED else 1
    except (OSError, ValueError, TypeError, AttributeError):
        print('RunBudgetRejected: invalid private mc request', file=sys.stderr)
        return 75
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
