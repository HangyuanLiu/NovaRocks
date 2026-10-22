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
"""Single-invocation SQL classification from native facts, never terminal text."""
import argparse
from collections.abc import Mapping
import datetime
import json
from pathlib import Path
import sys

from .compose import compose
from .dispositions import Dispositions
from .materials import read_sql, sql_scope_identity
from .model import Attempt, Scope, UnitKey


def classify(path, dispositions=None, admission_membership=False):
    path = Path(path)
    disposition = dispositions if isinstance(dispositions, Dispositions) else Dispositions(dispositions)
    observations, errors = read_sql(path, path.stem)
    # Selection is authoritative even when execution has no terminal records.
    records = []
    try:
        for line in path.read_bytes().splitlines(keepends=True):
            if not line.endswith(b'\n'):
                break
            records.append(json.loads(line))
    except (OSError, ValueError) as error:
        errors.append(str(error))
    run = records[0] if records and records[0].get('kind') == 'run' else {}
    selection = next((row for row in records if row.get('kind') == 'selection'), {})
    # A code observed while a negative oracle fails is not a direct refusal.
    # Other failed oracles remain independent facts after the primary refusal;
    # missing oracles alone are not failures because the refusal may stop work.
    assertion_failures = {}
    independent_assertions = {}
    for observation in observations:
        oracle = observation.detail.get('oracle')
        executed = oracle.get('executed', []) if isinstance(oracle, Mapping) else []
        failed = [value for value in executed
                  if value.get('outcome') == 'fail']
        if failed:
            key = (str(observation.unit), observation.lifecycle, observation.ref)
            assertion_failures[key] = failed
            # G15 also supports an exact registered result mismatch. Its own
            # failed result oracle is the primary fact, not another failure.
            primary = observation.failure or {}
            covered = primary.get('class') == 'mismatch' and disposition.match(observation)
            independent = [value for value in failed if not (
                covered and value.get('kind') == 'result_set'
                and value.get('step') == primary.get('statement_index'))]
            if independent:
                independent_assertions[key] = independent
    scopes = []
    for suite in selection.get('suites', []):
        units = tuple(UnitKey('sql-case', suite['suite'] + '/' + case['case']) for case in suite['cases'])
        if not units:
            continue
        declaration = suite.get('declaration', {})
        # Local CI is a rehearsal. It does not mint formal-attempt authority.
        scopes.append(Scope('sql-suite:' + suite['suite'], units,
            sql_scope_identity(run, selection, suite), declaration.get('initial_state'),
            declaration.get('env_inputs') is not None))
    # The raw reader respects case-scoped declarations. Normalize grouping only
    # for this invocation summary; no cross-lifecycle or cross-attempt reuse.
    for observation in observations:
        observation.scope = 'sql-suite:' + observation.unit.id.split('/', 1)[0]
    attempt = Attempt(path.stem, observations, current=True, errors=errors)
    composition = compose({}, 'dev', [attempt], scopes, disposition)
    # A local admission rehearsal uses the same membership exclusions while
    # retaining its local source. It never enters the formal observation pool.
    excluded = {unit: disposition.excluded(unit) for scope in scopes for unit in scope.units
                if admission_membership and disposition.excluded(unit)}
    gate_scopes = [Scope(scope.key, tuple(unit for unit in scope.units if unit not in excluded),
                         scope.identity, scope.initial_state, scope.inputs_declared) for scope in scopes
                   if any(unit not in excluded for unit in scope.units)]
    gate = compose({}, 'dev', [attempt], gate_scopes, disposition) if excluded else composition
    rows = []
    for item in composition['scopes']:
        for raw in item['observations']:
            unit = UnitKey.parse(raw['unit'])
            failed_oracles = assertion_failures.get((raw['unit'], raw['lifecycle'], raw['ref']), [])
            primary = raw.get('failure') or {}
            failed_primary_assertion = (primary.get('class') == 'error_code' and any(
                value.get('step') == primary.get('statement_index') for value in failed_oracles))
            if raw['category'] not in ('pass', 'counterexample'):
                status = 'NOT_RUN' if raw['category'] == 'coverage_gap' else 'DIAGNOSTIC'
            elif raw['category'] == 'pass':
                status = 'STALE_KNOWN_FAILURE' if any(entry['unit'] == str(unit) for _, _, _, entry in disposition.known_failures) else 'PASS'
            elif failed_primary_assertion:
                status = 'NEW_FAILURE'
            elif disposition.match(next(value for value in observations if value.ref == raw['ref'] and str(value.unit) == raw['unit'] and value.lifecycle == raw['lifecycle'])):
                status = 'KNOWN_FAIL'
            else:
                expired = Dispositions({'schema':1, 'known_failures':[entry for _, _, _, entry in disposition.known_failures], 'quarantine':[]}, datetime.date.min)
                native = next(value for value in observations if value.ref == raw['ref'] and str(value.unit) == raw['unit'] and value.lifecycle == raw['lifecycle'])
                status = 'EXPIRED_KNOWN_FAILURE' if expired.match(native) else 'NEW_FAILURE'
            rows.append({'unit':str(unit),'suite':unit.id.split('/',1)[0],'case':unit.id.split('/',1)[1],
                'status':'QUARANTINED' if unit in excluded else status, 'raw_status':status,
                'native_failure':raw['category']=='counterexample' and raw['record_complete'] and raw['terminated'],
                'failure':raw.get('failure'),'ref':raw['ref']})
    status = 'FAIL'
    cleanup_ok = not any(item['cleanup_obligations'] for item in composition['scopes'])
    if scopes and gate['layer_ok'] and cleanup_ok and not errors:
        status = 'UNEXPECTED_PASS' if any(row['status'] == 'STALE_KNOWN_FAILURE' for row in rows) else 'KNOWN_FAIL' if any(item['verdict'] == 'known_failure' for item in gate['scopes']) else 'PASS'
    end = next((row for row in reversed(records) if row.get('kind') == 'end'), {})
    # A single-invocation summary additionally describes the actual invocation.
    # An unexplained nonzero exit must not turn into PASS from earlier case rows.
    if end.get('completed') is not True or (end.get('exit_code') not in (0, 1)):
        status = 'FAIL'
    explained_case_failure = any(row['native_failure'] and row['status'] in ('KNOWN_FAIL','QUARANTINED') for row in rows)
    if end.get('exit_code') and status in ('PASS','KNOWN_FAIL') and not explained_case_failure:
        status = 'FAIL'
    if end.get('exit_code') == 0 and any(row['native_failure'] for row in rows):
        status = 'FAIL'
    strict_failures = [
        {'unit':unit, 'lifecycle':lifecycle, 'ref':ref, 'failed_oracles':failed}
        for (unit, lifecycle, ref), failed in independent_assertions.items()
        if UnitKey.parse(unit) not in excluded]
    strict_failures += [{'suite':row.get('suite'), 'init':row['init']}
                        for row in records if row.get('kind') == 'suite'
                        and row.get('init', {}).get('status') == 'failed']
    if strict_failures:
        status = 'FAIL'
    return {'schema':1,'status':status,'rows':rows,'errors':errors,'composition':composition,
            'strict_failures':strict_failures,
            'membership':{'layer':'admission' if admission_membership else 'dev',
                          'excluded':{str(unit):entry for unit,entry in excluded.items()}},
            'native_exit_code':end.get('exit_code'),'source':'local-rehearsal'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', type=Path, required=True)
    parser.add_argument('--dispositions', type=Path, required=True)
    parser.add_argument('--admission-membership', action='store_true')
    options = parser.parse_args(argv)
    result = classify(options.results, Dispositions.read(options.dispositions), options.admission_membership)
    print(json.dumps(result, sort_keys=True))
    return 0 if result['status'] in ('PASS', 'KNOWN_FAIL') else 1


if __name__ == '__main__':
    sys.exit(main())
