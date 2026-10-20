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
"""Development-only impact selection. Admission never calls this module."""
import fnmatch
from pathlib import Path
import subprocess

from .declarations import native_selector_requests


RUST_AGGREGATE = "workspace nextest and doctests"
RUST_AGGREGATE_DEFERRED = {
    "reason": "development Rust obligations use individual native scopes; workspace aggregation is deferred",
    "backstop": "admission",
}


def matches(path, patterns):
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def domain_intersects(directory, patterns):
    """Whether a declared path glob can reach a known Cargo package subtree.

    Prefixes are checked with the same glob matching as direct impact rules.
    This widens a missing mapping within one known owner, without constructing
    a Rust dependency graph or selecting unrelated domains.
    """
    for pattern in patterns:
        if matches(directory, [pattern]) or matches(directory + '/', [pattern]):
            return True
        parts = pattern.split('/')
        if any(fnmatch.fnmatchcase(directory, '/'.join(parts[:end]))
               for end in range(1, len(parts) + 1)):
            return True
    return False


def changed_paths(repo, base):
    def git(*args):
        return subprocess.check_output(['git'] + list(args), cwd=repo, text=True)
    merge = git('merge-base', 'HEAD', base).strip()
    # Preserve both old and new owners for a move between package domains.
    tracked = git('diff', '--no-renames', '--name-only', '-z', merge, '--').split('\0')
    untracked = git('ls-files', '--others', '--exclude-standard', '-z').split('\0')
    return sorted(set(filter(None, tracked + untracked)))


def package_directories(metadata, repo):
    repo = Path(repo).resolve()
    return {package['name']: str(Path(package['manifest_path']).resolve().parent.relative_to(repo))
            for package in metadata['packages'] if package['id'] in metadata['workspace_members']}


def ignored_scope_declarations(metadata):
    """Native selectors for ignored scopes; helper/manual entries are not units.

    Scope identities and affected reverse dependencies come from native list
    results in the caller. No unit is invented from a filter expression here.
    """
    packages = {package['id']: package for package in metadata['packages']}
    result = []
    for request in native_selector_requests(metadata):
        package = packages[request['package_id']]
        declaration = (package.get('metadata') or {}).get('novarocks', {}).get('test', {})
        entry = declaration['ignored'][request['index']]
        if entry.get('class') not in {'heavy', 'helper', 'manual'}:
            raise ValueError('unknown ignored classification')
        result.append(dict(entry, package=package['name'], package_id=package['id'],
            native_request_key=request['key'], filterset=request['filterset'],
            kind='rust-test', mode='default', tier='heavy' if entry['class']=='heavy' else None,
            validation_unit=entry['class']=='heavy'))
    return result


def select(paths, metadata, sql, system, stages, rules, repo):
    paths = sorted(set(paths))
    directories = package_directories(metadata, repo)
    workspace = {package['id'] for package in metadata['packages'] if package['id'] in metadata['workspace_members']}
    runtime_inputs = {package['name']: (package.get('metadata') or {}).get('novarocks', {}).get('test', {}).get('runtime-inputs', [])
                      for package in metadata['packages'] if package['id'] in workspace}
    selected_packages, selected_sql, selected_system, selected_stages = set(), {}, {}, {}
    gaps, reasons, broad = [], [], False
    for path in paths:
        owners = [name for name, directory in directories.items()
                  if directory != '.' and (path == directory or path.startswith(directory + '/'))]
        owner = max(owners, key=lambda name: len(directories[name])) if owners else None
        covered = False
        if owner:
            selected_packages.add(owner)
        for package, patterns in runtime_inputs.items():
            if matches(path, patterns):
                selected_packages.add(package)
                covered = True
                reasons.append({'path':path,'kind':'rust-package','name':package,
                    'reason':'declared runtime input; native rdeps selection'})
        # SQL files always select their native suite, even without an impact declaration.
        parts = path.split('/')
        native_suite = parts[3] if len(parts) > 3 and parts[:3] == ['tests', 'sql', 'correctness'] else None
        for declaration in sql.get('suites', []):
            name = declaration['name']
            if name == native_suite or matches(path, declaration.get('impact', {}).get('paths', [])):
                selected_sql[name] = declaration
                covered = True
                reasons.append({'path':path,'kind':'sql-suite','name':name,'reason':'native suite input' if name == native_suite else 'suite impact declaration'})
        for declaration in system.get('scenarios', []):
            if matches(path, declaration.get('declaration', {}).get('impact', {}).get('paths', [])):
                selected_system[declaration['name']] = declaration
                covered = True
                reasons.append({'path':path,'kind':'system-scenario','name':declaration['name'],'reason':'scenario impact declaration'})
        for declaration in stages.get('stages', []):
            if matches(path, declaration.get('domain', [])):
                selected_stages[declaration['name']] = declaration
                reasons.append({'path':path,'kind':'stage','name':declaration['name'],'reason':'stage domain declaration'})
                # Generic repository-wide checks do not conceal a domain mapping gap.
                covered |= declaration.get('domain') != ['**']
        for rule in rules.get('rules', []):
            if matches(path, rule['paths']):
                reasons.append({'path':path,'kind':'rule','reason':rule['reason']})
                if rule['action'] == 'broad':
                    covered = True
                    broad = True
                elif rule['action'] == 'documentation':
                    covered = True
                elif rule['action'] != 'declared':
                    raise ValueError('unknown impact action')
                # A delegation rule alone is not an actual test mapping.
        if owner:
            reasons.append({'path':path,'kind':'rust-package','name':owner,'reason':'Cargo package ownership; native rdeps selection'})
        if not covered:
            gaps.append({'path':path,'owner':owner,'expansion':'package-domain' if owner else 'broad',
                         'domain':directories[owner] if owner else None,
                         'reason':'no explicit impact declaration covers this path'})
            if not owner:
                broad = True
            else:
                directory = directories[owner]
                groups = (
                    ('sql-suite', sql.get('suites', []), selected_sql,
                     lambda value: value.get('impact', {}).get('paths', [])),
                    ('system-scenario', system.get('scenarios', []), selected_system,
                     lambda value: value.get('declaration', {}).get('impact', {}).get('paths', [])),
                    ('stage', stages.get('stages', []), selected_stages,
                     lambda value: value.get('domain', [])))
                for kind, declarations, selected, patterns in groups:
                    for declaration in declarations:
                        if domain_intersects(directory, patterns(declaration)):
                            name = declaration['name']
                            selected[name] = declaration
                            reasons.append({'path':path,'kind':kind,'name':name,'owner':owner,
                                'reason':'unmapped path widens to the Cargo package domain'})
    if broad:
        selected_packages.update(directories)
        selected_sql = {item['name']: item for item in sql.get('suites', [])}
        selected_system = {item['name']: item for item in system.get('scenarios', [])}
        selected_stages = {item['name']: item for item in stages.get('stages', [])}
    # Keep the native declaration intact; only the development projection changes.
    aggregate = selected_stages.pop(RUST_AGGREGATE, None)
    deferred_stages = ([dict(aggregate, deferred=dict(RUST_AGGREGATE_DEFERRED))]
                       if aggregate else [])
    heavy = [dict(entry, author_obligation=True, selection_basis='direct-owner')
             for entry in ignored_scope_declarations(metadata)
             if entry['validation_unit'] and entry['package'] in selected_packages]
    # Cargo names are valid nextest exact package names. Reverse dependencies
    # are evaluated by nextest, including dependent packages' heavy tests.
    rust_filter = ' | '.join('rdeps(={})'.format(name) for name in sorted(selected_packages))
    return {'schema':1,'changed_paths':sorted(paths),'packages':sorted(selected_packages),
            'rust_filterset':rust_filter or None,'doctest':bool(selected_packages),
            'sql_suites':[selected_sql[name] for name in sorted(selected_sql)],
            'system_scenarios':[selected_system[name] for name in sorted(selected_system)],
            'stages':[selected_stages[name] for name in sorted(selected_stages)],
            'deferred_stages':deferred_stages,
            'heavy_ignored':heavy,'heavy_ignored_summary_basis':'direct-owner',
            'heavy_ignored_requires_native_resolution':bool(rust_filter),
            'gaps':gaps,'broad':broad,'reasons':reasons}


def sql_case_arguments(declaration, cases):
    reuse = declaration.get('reuse') or declaration.get('declaration') or {}
    if cases is None:
        return []
    if reuse.get('scope') != 'case' or reuse.get('initial_state') not in {'isolated','reset','immutable'}:
        raise ValueError('shared-lifecycle SQL suite cannot use --only')
    return ['--only', ','.join(cases)]
